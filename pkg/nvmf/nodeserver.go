/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nvmf

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"
)

type NodeServer struct {
	csi.UnimplementedNodeServer
	Driver  *driver
	mounter mount.Interface
}

func NewNodeServer(d *driver) *NodeServer {
	return &NodeServer{
		Driver:  d,
		mounter: mount.New(""),
	}
}

func isEphemeral(volumeContext map[string]string) bool {
	return volumeContext["csi.storage.k8s.io/ephemeral"] == "true"
}

func getPodName(volumeContext map[string]string) string {
	if name, ok := volumeContext["csi.storage.k8s.io/pod/name"]; ok {
		return name
	}
	return "unknown-pod"
}

func ensureDir(p string) error {
	return os.MkdirAll(p, 0750)
}

func ensureFile(p string) error {
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		return err
	}
	// Create if missing, but don't truncate. Must include a write flag with O_CREATE.
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}

// bindMount mounts source -> target with bind (+ optional ro remount).
// Uses SafeFormatAndMount's IsLikelyNotMountPoint for robust checks across file/dir targets.
func bindMount(mounter mount.Interface, source, target string, readOnly bool) error {
	safe := &mount.SafeFormatAndMount{Interface: mounter, Exec: exec.New()}

	// If already mounted, ensure ro if requested and return success.
	notMounted, err := safe.IsLikelyNotMountPoint(target)
	if err == nil && !notMounted {
		if readOnly {
			// For remount, use target as "source" (safer across implementations).
			_ = mounter.Mount(target, target, "", []string{"remount", "bind", "ro"})
		}
		return nil
	}

	// Attempt bind mount.
	if err := mounter.Mount(source, target, "", []string{"bind"}); err != nil {
		// Tolerate races: re-check mountpoint; if now mounted, treat as success.
		nm, chkErr := safe.IsLikelyNotMountPoint(target)
		if chkErr == nil && !nm {
			if readOnly {
				_ = mounter.Mount(target, target, "", []string{"remount", "bind", "ro"})
			}
			return nil
		}
		return err
	}

	// Enforce RO if requested.
	if readOnly {
		if err := mounter.Mount(target, target, "", []string{"remount", "bind", "ro"}); err != nil {
			return err
		}
	}
	return nil
}

// stageVolume:
// - filesystem: format+mount device at stagingTargetPath
// - block: bind-mount devicePath onto <stagingTargetPath>/block (file)
func (n *NodeServer) stageVolume(req *csi.NodeStageVolumeRequest, devicePath string) error {
	staging := req.GetStagingTargetPath()
	if staging == "" {
		return fmt.Errorf("stagingTargetPath is required")
	}
	if err := ensureDir(staging); err != nil {
		return fmt.Errorf("failed to create stagingTargetPath %s: %w", staging, err)
	}

	cap := req.GetVolumeCapability()
	if cap == nil {
		return fmt.Errorf("volume capability missing")
	}

	// BLOCK
	if isBlock(cap) {
		stageFile := filepath.Join(staging, "block")
		if err := ensureFile(stageFile); err != nil {
			return fmt.Errorf("failed to create staging block file %s: %w", stageFile, err)
		}

		// Idempotent: if already mounted, return success.
		safe := &mount.SafeFormatAndMount{Interface: n.mounter, Exec: exec.New()}
		notMounted, err := safe.IsLikelyNotMountPoint(stageFile)
		if err == nil && !notMounted {
			return nil
		}

		if err := bindMount(n.mounter, devicePath, stageFile, false); err != nil {
			return fmt.Errorf("failed to stage block bind mount %s -> %s: %w", devicePath, stageFile, err)
		}
		return nil
	}

	// FILESYSTEM
	if isMount(cap) {
		safe := &mount.SafeFormatAndMount{Interface: n.mounter, Exec: exec.New()}

		fsType := ""
		if req.GetVolumeContext() != nil {
			fsType = req.GetVolumeContext()["csi.storage.k8s.io/fstype"]
		}
		if fsType == "" {
			fsType = cap.GetMount().GetFsType()
		}
		if fsType == "" {
			fsType = "ext4"
		}

		opts := append([]string{}, cap.GetMount().GetMountFlags()...)
		opts = append(opts, "rw") // stage as rw; publish enforces ro via bind remount if requested

		notMounted, err := safe.IsLikelyNotMountPoint(staging)
		if err == nil && !notMounted {
			return nil
		}

		if err := safe.FormatAndMount(devicePath, staging, fsType, opts); err != nil {
			return fmt.Errorf("failed to stage mount %s -> %s (%s): %w", devicePath, staging, fsType, err)
		}
		return nil
	}

	return fmt.Errorf("unsupported volume capability type")
}

// publishFromStaging:
// - filesystem: bind-mount staging dir -> targetPath
// - block: bind-mount <staging>/block file -> targetPath
func (n *NodeServer) publishFromStaging(req *csi.NodePublishVolumeRequest) error {
	staging := req.GetStagingTargetPath()
	target := req.GetTargetPath()

	if staging == "" {
		return fmt.Errorf("stagingTargetPath required")
	}
	if target == "" {
		return fmt.Errorf("targetPath required")
	}

	cap := req.GetVolumeCapability()
	if cap == nil {
		return fmt.Errorf("volume capability missing")
	}

	readOnly := req.GetReadonly()

	// BLOCK
	if isBlock(cap) {
		if err := ensureFile(target); err != nil {
			return fmt.Errorf("failed to ensure block target file %s: %w", target, err)
		}
		stageFile := filepath.Join(staging, "block")
		if err := bindMount(n.mounter, stageFile, target, readOnly); err != nil {
			return fmt.Errorf("failed to publish block bind mount %s -> %s: %w", stageFile, target, err)
		}
		return nil
	}

	// FILESYSTEM
	if isMount(cap) {
		if err := ensureDir(target); err != nil {
			return fmt.Errorf("failed to ensure mount target dir %s: %w", target, err)
		}
		if err := bindMount(n.mounter, staging, target, readOnly); err != nil {
			return fmt.Errorf("failed to publish bind mount %s -> %s: %w", staging, target, err)
		}
		return nil
	}

	return fmt.Errorf("unsupported volume capability type")
}

func (n *NodeServer) unpublishTarget(target string) error {
	safe := &mount.SafeFormatAndMount{Interface: n.mounter, Exec: exec.New()}

	notMounted, err := safe.IsLikelyNotMountPoint(target)
	if err == nil && !notMounted {
		if umErr := safe.Unmount(target); umErr != nil {
			// Return error so kubelet retries; prevents mount leaks.
			return fmt.Errorf("failed to unmount target %s: %w", target, umErr)
		}
	}

	// Cleanup only the target itself (file for block, dir for fs). Avoid parent dirs.
	_ = os.RemoveAll(target)
	return nil
}

// unstage cleans up the staging path.
// Since NodeUnstageVolumeRequest doesn't include cap, we always try block-stage-file first.
func (n *NodeServer) unstage(staging string, cap *csi.VolumeCapability) error {
	safe := &mount.SafeFormatAndMount{Interface: n.mounter, Exec: exec.New()}
	if staging == "" {
		return nil
	}

	// Try block staging file first (covers unknown cap case).
	stageFile := filepath.Join(staging, "block")
	if _, statErr := os.Stat(stageFile); statErr == nil {
		notMounted, err := safe.IsLikelyNotMountPoint(stageFile)
		if err == nil && !notMounted {
			if umErr := safe.Unmount(stageFile); umErr != nil {
				return fmt.Errorf("failed to unmount staging block file %s: %w", stageFile, umErr)
			}
		}
		_ = os.RemoveAll(stageFile)
	}

	// If cap explicitly says block, we're done after stageFile cleanup.
	if cap != nil && isBlock(cap) {
		_ = os.RemoveAll(staging)
		return nil
	}

	// Treat staging as directory mount for filesystem staging.
	notMounted, err := safe.IsLikelyNotMountPoint(staging)
	if err == nil && !notMounted {
		if umErr := safe.Unmount(staging); umErr != nil {
			return fmt.Errorf("failed to unmount staging path %s: %w", staging, umErr)
		}
	}

	_ = os.RemoveAll(staging)
	return nil
}

func (n *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Infof("NodeGetCapabilities called")
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME}}},
			{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME}}},
			{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS}}},
			{Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP}}},
		},
	}, nil
}

func (n *NodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath missing in request")
	}

	isEph := isEphemeral(req.GetVolumeContext())
	podName := getPodName(req.GetVolumeContext())
	if isEph {
		klog.Infof("NodeStageVolume: Ephemeral volume %s for pod %s", volumeID, podName)
	}

	// Build diskInfo from the stage request (use CSI-correct accessors)
	publishLikeReq := &csi.NodePublishVolumeRequest{
		VolumeId:         volumeID,
		VolumeContext:    req.GetVolumeContext(),
		VolumeCapability: req.GetVolumeCapability(),
	}
	diskInfo, err := getNVMfDiskInfo(publishLikeReq)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed to extract NVMe info: %v", err)
	}

	klog.V(4).Infof("NodeStageVolume: connecting volume %s (addr=%s, port=%s, nqn=%s)%s",
		volumeID, diskInfo.Addr, diskInfo.Port, diskInfo.Nqn,
		map[bool]string{true: " (ephemeral)", false: ""}[isEph])

	connector := getNvmfConnector(diskInfo)
	devicePath, err := connector.Connect()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect volume %s: %v", volumeID, err)
	}
	if devicePath == "" {
		return nil, status.Errorf(codes.Internal, "connect succeeded but returned empty device path for volume %s", volumeID)
	}
	klog.V(4).Infof("NodeStageVolume: volume %s connected at device %s", volumeID, devicePath)

	// Persist connector info (so Unstage/Expand can recover NQN/device even after restart)
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if err := persistConnectorFile(connector, connectorFilePath); err != nil {
		// Best-effort disconnect; return error so kubelet retries.
		_ = connector.Disconnect()
		_ = os.Remove(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "failed to persist connector for volume %s: %v", volumeID, err)
	}

	// Stage mount/bind.
	if err := n.stageVolume(req, devicePath); err != nil {
		// IMPORTANT: do NOT disconnect here. Many staging errors are transient and kubelet will retry.
		// Keeping the connection + connector file avoids connect/disconnect flapping.
		return nil, status.Errorf(codes.Internal, "failed to stage volume %s: %v", volumeID, err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath missing in request")
	}

	staging := req.GetStagingTargetPath()

	// Unstage (return errors so kubelet retries; prevents mount leaks)
	if err := n.unstage(staging, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unstage volume %s: %v", volumeID, err)
	}

	// Disconnect using persisted connector info.
	// IMPORTANT: only remove connector file if disconnect succeeded.
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	connector, err := GetConnectorFromFile(connectorFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to read connector file for volume %s: %v", volumeID, err)
	}

	if derr := connector.Disconnect(); derr != nil {
		// Keep connector file so kubelet retry can attempt again.
		return nil, status.Errorf(codes.Internal, "failed to disconnect volume %s: %v", volumeID, derr)
	}

	removeConnectorFile(connectorFilePath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *NodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Target Path missing in request")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "StagingTargetPath missing in request")
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	isEph := isEphemeral(req.GetVolumeContext())
	podName := getPodName(req.GetVolumeContext())

	if isEph {
		klog.Infof("NodePublishVolume: Publishing ephemeral volume %s for pod %s to %s", volumeID, podName, targetPath)
	} else {
		klog.V(4).Infof("NodePublishVolume: publishing volume %s to %s", volumeID, targetPath)
	}

	if err := n.publishFromStaging(req); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to publish volume %s: %v", volumeID, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Target Path missing in request")
	}

	if err := n.unpublishTarget(req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unpublish volume %s: %v", req.GetVolumeId(), err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()
	requiredBytes := int64(0)
	if req.GetCapacityRange() != nil {
		requiredBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	klog.V(4).Infof("NodeExpandVolume called for volume %s (volumePath=%s, requiredBytes=%d)", volumeID, volumePath, requiredBytes)

	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")

	connector, loadErr := GetConnectorFromFile(connectorFilePath)
	if loadErr != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to load connector for volume %s: %v", volumeID, loadErr)
	}
	if connector.TargetNqn == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "connector missing TargetNqn for volume %s", volumeID)
	}
	nqn := connector.TargetNqn

	// Lookup device by NQN (do NOT Connect() here)
	_, devicePath, derr := getNvmeInfoByNqn(nqn)
	if derr != nil || devicePath == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "device not found for volume %s (nqn=%s): %v", volumeID, nqn, derr)
	}

	// Rescan controller(s) that match this NQN
	const ctlPath = "/sys/class/nvme-fabrics/ctl"
	if entries, err := os.ReadDir(ctlPath); err == nil {
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "nvme") {
				continue
			}
			subsysPath := filepath.Join(ctlPath, entry.Name(), "subsysnqn")
			data, err := os.ReadFile(subsysPath)
			if err != nil {
				continue
			}
			if strings.TrimSpace(string(data)) == nqn {
				scanPath := filepath.Join(ctlPath, entry.Name(), "rescan_controller")
				if utils.IsFileExisting(scanPath) {
					if file, err := os.OpenFile(scanPath, os.O_WRONLY, 0666); err == nil {
						defer file.Close()
						_, _ = file.WriteString("1")
						klog.V(4).Infof("Triggered rescan for controller %s (nqn=%s)", entry.Name(), nqn)
					}
				}
			}
		}
	}

	// Online filesystem resize if mounted (volumePath is target mount path for fs volumes)
	if volumePath != "" {
		if st, err := os.Stat(volumePath); err == nil && st.IsDir() {
			fsType := getFSType(devicePath)
			klog.V(4).Infof("Detected filesystem type '%s' on device %s", fsType, devicePath)

			var out []byte
			var cmdErr error
			exe := exec.New()

			if fsType == "xfs" {
				out, cmdErr = exe.Command("xfs_growfs", volumePath).CombinedOutput()
			} else if strings.HasPrefix(fsType, "ext") {
				out, cmdErr = exe.Command("resize2fs", devicePath).CombinedOutput()
			} else {
				klog.V(5).Infof("No online resize attempted for filesystem type '%s'", fsType)
			}

			if cmdErr != nil {
				klog.Errorf("Filesystem resize failed: %v\nOutput: %s", cmdErr, string(out))
				return nil, status.Errorf(codes.Internal, "filesystem resize failed: %v", cmdErr)
			}
			if len(out) > 0 {
				klog.V(4).Infof("Filesystem resize success: %s", string(out))
			}
		}
	}

	// Return actual size when possible (CSI expects current capacity after expansion).
	capBytes := requiredBytes
	if sz, szErr := getBlockSizeBytes(devicePath); szErr == nil && sz > 0 {
		capBytes = sz
	}

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: capBytes,
	}, nil
}

func getBlockSizeBytes(devicePath string) (int64, error) {
	exe := exec.New()
	out, err := exe.Command("blockdev", "--getsize64", devicePath).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("blockdev --getsize64 %s failed: %v (out=%s)", devicePath, err, strings.TrimSpace(string(out)))
	}
	s := strings.TrimSpace(string(out))
	v, convErr := strconv.ParseInt(s, 10, 64)
	if convErr != nil {
		return 0, fmt.Errorf("parse blockdev size %q: %w", s, convErr)
	}
	return v, nil
}

func getFSType(devicePath string) string {
	exe := exec.New()
	out, err := exe.Command("blkid", "-o", "value", "-s", "TYPE", devicePath).CombinedOutput()
	if err != nil {
		klog.V(5).Infof("blkid failed for %s: %v", devicePath, err)
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (n *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(5).Infof("NodeGetInfo called")
	topology := &csi.Topology{Segments: map[string]string{}}

	if zone := os.Getenv("CSI_NODE_ZONE"); zone != "" {
		topology.Segments["topology.nvmf.csi/zone"] = zone
	}
	if region := os.Getenv("CSI_NODE_REGION"); region != "" {
		topology.Segments["topology.nvmf.csi/region"] = region
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             n.Driver.nodeId,
		MaxVolumesPerNode:  0,
		AccessibleTopology: topology,
	}, nil
}

func (n *NodeServer) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}

	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	st, err := os.Stat(volumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "Volume path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "Failed to stat volume path %s: %v", volumePath, err)
	}

	condition := &csi.VolumeCondition{Abnormal: false, Message: "Volume is healthy"}
	var usage []*csi.VolumeUsage

	// Resolve NQN from persisted connector file (VolumeID != NQN)
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	var nqn string
	if connector, lerr := GetConnectorFromFile(connectorFilePath); lerr == nil && connector != nil && connector.TargetNqn != "" {
		nqn = connector.TargetNqn
	} else if lerr != nil && !os.IsNotExist(lerr) {
		condition.Abnormal = true
		condition.Message = "Failed to read persisted connector state"
		klog.Warningf("Failed to read connector file for volume %s: %v", volumeID, lerr)
	}

	// Best-effort resolve NVMe block device via NQN (if available)
	var nqnDev string
	if nqn != "" {
		_, dev, derr := getNvmeInfoByNqn(nqn)
		if derr != nil || dev == "" {
			condition.Abnormal = true
			condition.Message = "Underlying NVMe device disconnected or unavailable"
			klog.Warningf("Volume %s condition abnormal: device lookup failed (nqn=%s, err=%v)", volumeID, nqn, derr)
		} else {
			nqnDev = dev
		}
	}

	if st.IsDir() {
		var statfs syscall.Statfs_t
		if err := syscall.Statfs(volumePath, &statfs); err != nil {
			condition.Abnormal = true
			condition.Message = "Failed to retrieve filesystem statistics"
			klog.Errorf("Failed to get filesystem stats for %s: %v", volumePath, err)
		} else {
			totalBytes := int64(statfs.Blocks * uint64(statfs.Bsize))
			availableBytes := int64(statfs.Bfree * uint64(statfs.Bsize))
			usedBytes := totalBytes - availableBytes
			totalInodes := int64(statfs.Files)
			freeInodes := int64(statfs.Ffree)
			usedInodes := totalInodes - freeInodes

			usage = []*csi.VolumeUsage{
				{Unit: csi.VolumeUsage_BYTES, Total: totalBytes, Available: availableBytes, Used: usedBytes},
				{Unit: csi.VolumeUsage_INODES, Total: totalInodes, Available: freeInodes, Used: usedInodes},
			}
		}
	} else {
		// Block stats: prefer the resolved NVMe device (nqnDev), else fall back to volumePath.
		sizePath := nqnDev
		if sizePath == "" {
			sizePath = volumePath
		}

		size, derr := getBlockSizeBytes(sizePath)
		if derr != nil {
			// Keep it best-effort to avoid noisy false alarms; you can flip to Abnormal=true if you prefer strict.
			klog.V(4).Infof("Best-effort block size lookup failed for %s (volume %s): %v", sizePath, volumeID, derr)
			size = 0
		}

		usage = []*csi.VolumeUsage{
			{Unit: csi.VolumeUsage_BYTES, Total: size, Available: size, Used: 0},
		}
	}

	klog.V(4).Infof("NodeGetVolumeStats volume %s at %s done (abnormal=%v, msg=%s)",
		volumeID, volumePath, condition.Abnormal, condition.Message)

	return &csi.NodeGetVolumeStatsResponse{
		Usage:           usage,
		VolumeCondition: condition,
	}, nil
}

func isBlock(cap *csi.VolumeCapability) bool {
	return cap != nil && cap.GetBlock() != nil
}

func isMount(cap *csi.VolumeCapability) bool {
	return cap != nil && cap.GetMount() != nil
}
