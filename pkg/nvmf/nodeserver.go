/* Copyright 2021 The Kubernetes Authors.
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
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
	// Create if missing, but don't truncate.
	f, err := os.OpenFile(p, os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	return f.Close()
}

// bindMount mounts source -> target with bind (+ optional ro remount).
func bindMount(mounter mount.Interface, source, target string, readOnly bool) error {
	notMounted, err := mount.IsNotMountPoint(mounter, target)
	if err == nil && !notMounted {
		return nil
	}

	if err := mounter.Mount(source, target, "", []string{"bind"}); err != nil {
		return err
	}

	if readOnly {
		// Remount ro to enforce it (common best practice).
		if err := mounter.Mount(source, target, "", []string{"remount", "bind", "ro"}); err != nil {
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

	// NOTE: NodeStageVolumeRequest does NOT have Readonly; enforce RO at Publish.
	stageReadOnly := false

	// ===== BLOCK STAGE =====
	if isBlock(cap) {
		stageFile := filepath.Join(staging, "block")
		if err := ensureFile(stageFile); err != nil {
			return fmt.Errorf("failed to create staging block file %s: %w", stageFile, err)
		}
		if err := bindMount(n.mounter, devicePath, stageFile, stageReadOnly); err != nil {
			return fmt.Errorf("failed to stage block bind mount %s -> %s: %w", devicePath, stageFile, err)
		}
		return nil
	}

	// ===== FILESYSTEM STAGE =====
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
		// stage as rw; publish may remount ro via bind remount
		opts = append(opts, "rw")

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

	// ===== BLOCK PUBLISH =====
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

	// ===== FILESYSTEM PUBLISH =====
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
			return fmt.Errorf("failed to unmount target %s: %w", target, umErr)
		}
	}

	// best-effort cleanup
	_ = os.Remove(target)
	_ = os.Remove(filepath.Dir(target))
	return nil
}

func (n *NodeServer) unstage(staging string, cap *csi.VolumeCapability) error {
	safe := &mount.SafeFormatAndMount{Interface: n.mounter, Exec: exec.New()}

	if staging == "" {
		return nil
	}

	if cap != nil && isBlock(cap) {
		stageFile := filepath.Join(staging, "block")
		notMounted, err := safe.IsLikelyNotMountPoint(stageFile)
		if err == nil && !notMounted {
			_ = safe.Unmount(stageFile)
		}
		_ = os.Remove(stageFile)
	} else {
		notMounted, err := safe.IsLikelyNotMountPoint(staging)
		if err == nil && !notMounted {
			_ = safe.Unmount(staging)
		}
	}

	_ = os.Remove(staging)
	return nil
}

func (n *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Infof("NodeGetCapabilities called")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_VOLUME_MOUNT_GROUP},
				},
			},
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

	// Persist connector info (so Unstage can disconnect even after restart)
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if err := persistConnectorFile(connector, connectorFilePath); err != nil {
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "failed to persist connector for volume %s: %v", volumeID, err)
	}

	// CSI-textbook staging: mount/prepare at stagingTargetPath
	if err := n.stageVolume(req, devicePath); err != nil {
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
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

	isEph := strings.HasPrefix(volumeID, "ephemeral-")
	klog.V(4).Infof("NodeUnstageVolume: unstaging+disconnecting volume %s%s", volumeID,
		map[bool]string{true: " (ephemeral)", false: ""}[isEph])

	// NodeUnstageVolumeRequest doesn't carry VolumeCapability (CSI v1.12), so best-effort unstage:
	_ = n.unstage(req.GetStagingTargetPath(), nil)

	// Disconnect using persisted connector info (best-effort)
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	connector, err := GetConnectorFromFile(connectorFilePath)
	if err == nil {
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		klog.V(4).Infof("NodeUnstageVolume: disconnected and cleaned up persisted info for volume %s", volumeID)
	} else {
		klog.V(5).Infof("NodeUnstageVolume: no persisted connector found for volume %s (already cleaned)", volumeID)
	}

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

	// CSI-textbook publish: bind mount from staging -> target
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

	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()
	isEph := strings.HasPrefix(volumeID, "ephemeral-")

	klog.V(4).Infof("NodeUnpublishVolume: unpublishing volume %s at %s%s", volumeID, targetPath,
		map[bool]string{true: " (ephemeral)", false: ""}[isEph])

	if err := n.unpublishTarget(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unpublish volume %s: %v", volumeID, err)
	}

	klog.V(4).Infof("NodeUnpublishVolume: successfully unpublished volume %s", volumeID)
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
	var devicePath string
	var err error

	_, devicePath, err = getNvmeInfoByNqn(volumeID)
	if err == nil && devicePath != "" {
		klog.V(4).Infof("Found existing device %s for volume %s", devicePath, volumeID)
	} else {
		klog.V(4).Infof("Device lookup failed for volume %s, attempting recovery", volumeID)
		connector, loadErr := GetConnectorFromFile(connectorFilePath)
		if loadErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s not connected and recovery failed", volumeID)
		}
		recoveredDevice, reconErr := connector.Connect()
		if reconErr != nil || recoveredDevice == "" {
			return nil, status.Errorf(codes.Internal, "recovery reconnect failed for volume %s", volumeID)
		}
		for i := 0; i < 30; i++ {
			_, devicePath, err = getNvmeInfoByNqn(volumeID)
			if err == nil && devicePath != "" {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if devicePath == "" {
			return nil, status.Errorf(codes.Internal, "device not found after recovery for volume %s", volumeID)
		}
	}

	// Rescan controllers
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
			if strings.TrimSpace(string(data)) == volumeID {
				scanPath := filepath.Join(ctlPath, entry.Name(), "rescan_controller")
				if utils.IsFileExisting(scanPath) {
					if file, err := os.OpenFile(scanPath, os.O_WRONLY, 0666); err == nil {
						defer file.Close()
						_, _ = file.WriteString("1")
						klog.V(4).Infof("Triggered rescan for controller %s", entry.Name())
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

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: requiredBytes,
	}, nil
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

	var usage []*csi.VolumeUsage
	condition := &csi.VolumeCondition{Abnormal: false, Message: "Volume is healthy"}

	// Best-effort NVMe connection check
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if _, err := os.Stat(connectorFilePath); err == nil {
		_, devicePath, lookupErr := getNvmeInfoByNqn(volumeID)
		if lookupErr != nil || devicePath == "" {
			condition.Abnormal = true
			condition.Message = "Underlying NVMe device disconnected or unavailable"
			klog.Warningf("Volume %s condition abnormal: device lookup failed (%v)", volumeID, lookupErr)
		}
	} else if !os.IsNotExist(err) {
		klog.Warningf("Failed to stat persisted connector file for volume %s: %v", volumeID, err)
	}

	if st.IsDir() {
		var statfs syscall.Statfs_t
		if err := syscall.Statfs(volumePath, &statfs); err != nil {
			if !condition.Abnormal {
				condition.Abnormal = true
				condition.Message = "Failed to retrieve filesystem statistics"
			}
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
		// Block stats: best-effort size via Seek (works for block devices on Linux)
		fd, err := os.Open(volumePath)
		if err != nil {
			if !condition.Abnormal {
				condition.Abnormal = true
				condition.Message = "Failed to open block device"
			}
			return nil, status.Errorf(codes.Internal, "Failed to open block device %s: %v", volumePath, err)
		}
		defer fd.Close()

		size, seekErr := fd.Seek(0, io.SeekEnd)
		if seekErr != nil || size <= 0 {
			if !condition.Abnormal {
				condition.Abnormal = true
				condition.Message = "Failed to determine block device size"
			}
			klog.Warningf("Seek failed or returned <=0 for block device %s (err: %v)", volumePath, seekErr)
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
