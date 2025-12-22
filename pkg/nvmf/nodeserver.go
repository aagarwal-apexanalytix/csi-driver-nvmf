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

func (n *NodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Infof("NodeGetCapabilities called")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

func (n *NodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}

	diskInfo, err := getNVMfDiskInfo(&csi.NodePublishVolumeRequest{
		VolumeId:         volumeID,
		VolumeContext:    req.GetVolumeContext(),
		VolumeCapability: req.GetVolumeCapability(),
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed to extract NVMe info: %v", err)
	}

	klog.V(4).Infof("NodeStageVolume: connecting volume %s (addr=%s, port=%s, nqn=%s)", volumeID, diskInfo.Addr, diskInfo.Port, diskInfo.Nqn)

	connector := getNvmfConnector(diskInfo)
	devicePath, err := connector.Connect()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect volume %s: %v", volumeID, err)
	}
	if devicePath == "" {
		return nil, status.Errorf(codes.Internal, "connect succeeded but returned empty device path for volume %s", volumeID)
	}

	klog.V(4).Infof("NodeStageVolume: volume %s connected at device %s", volumeID, devicePath)

	// Persist connector info for recovery
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if err := persistConnectorFile(connector, connectorFilePath); err != nil {
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "failed to persist connector for volume %s: %v", volumeID, err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	klog.V(4).Infof("NodeUnstageVolume: disconnecting volume %s", volumeID)

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
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capability missing in request")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Target Path missing in request")
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	klog.V(4).Infof("NodePublishVolume: volume %s to target %s", volumeID, targetPath)

	// Get or recover connector
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	var devicePath string
	var connector *Connector

	conn, err := GetConnectorFromFile(connectorFilePath)
	if err != nil {
		// Fallback to direct connect from context
		klog.V(5).Infof("No persisted connector for %s, connecting directly", volumeID)
		diskInfo, extractErr := getNVMfDiskInfo(req)
		if extractErr != nil {
			return nil, extractErr
		}
		connector = getNvmfConnector(diskInfo)
	} else {
		connector = conn
	}

	devicePath, err = connector.Connect()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect volume %s: %v", volumeID, err)
	}
	if devicePath == "" {
		return nil, status.Errorf(codes.Internal, "connect succeeded but empty device path for volume %s", volumeID)
	}

	klog.V(4).Infof("NodePublishVolume: volume %s connected at device %s", volumeID, devicePath)

	// Persist if fallback (non-critical)
	if err := persistConnectorFile(connector, connectorFilePath); err != nil {
		klog.Warningf("Failed to persist connector for volume %s (non-critical): %v", volumeID, err)
	}

	if err := AttachDisk(req, devicePath); err != nil {
		// Rollback connection only if fallback
		if _, loadErr := GetConnectorFromFile(connectorFilePath); loadErr != nil {
			_ = connector.Disconnect()
			removeConnectorFile(connectorFilePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to publish volume %s: %v", volumeID, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Target Path missing in request")
	}

	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	klog.V(4).Infof("NodeUnpublishVolume: unpublishing volume %s at %s", volumeID, targetPath)

	if err := DetachDisk(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to detach volume %s: %v", volumeID, err)
	}

	klog.V(4).Infof("NodeUnpublishVolume: successfully unpublished volume %s", volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumeID := req.VolumeId
	volumePath := req.GetVolumePath()
	klog.V(4).Infof("NodeExpandVolume called for volume %s (volumePath=%s, requiredBytes=%d)", volumeID, volumePath, req.CapacityRange.GetRequiredBytes())

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

	// Online filesystem resize if mounted
	if volumePath != "" {
		if stat, err := os.Stat(volumePath); err == nil && stat.IsDir() {
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
		CapacityBytes: req.CapacityRange.GetRequiredBytes(),
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
	topology := &csi.Topology{
		Segments: map[string]string{},
	}
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
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}

	volumeID := req.VolumeId
	volumePath := req.VolumePath
	stat, err := os.Stat(volumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "Volume path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "Failed to stat volume path %s: %v", volumePath, err)
	}

	var usage []*csi.VolumeUsage
	condition := &csi.VolumeCondition{
		Abnormal: false,
		Message:  "Volume is healthy",
	}

	// Check underlying NVMe device connection health
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if _, err := os.Stat(connectorFilePath); err == nil {
		// Persisted connector exists — verify device is still present
		_, devicePath, lookupErr := getNvmeInfoByNqn(volumeID)
		if lookupErr != nil || devicePath == "" {
			condition.Abnormal = true
			condition.Message = "Underlying NVMe device disconnected or unavailable"
			klog.Warningf("Volume %s condition abnormal: device lookup failed (%v)", volumeID, lookupErr)
		}
	} else if !os.IsNotExist(err) {
		klog.Warningf("Failed to stat persisted connector file for volume %s: %v", volumeID, err)
	}

	// If no persisted file, assume healthy (common for non-staged or legacy)
	if stat.IsDir() {
		// Filesystem stats
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
		// Raw block volume stats (cross-platform fallback with Seek)
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
		if seekErr != nil || size == 0 {
			if !condition.Abnormal {
				condition.Abnormal = true
				condition.Message = "Failed to determine block device size"
			}
			klog.Warningf("Seek failed or returned 0 for block device %s (err: %v)", volumePath, seekErr)
			size = 0 // Report 0 if unknown
		}
		totalBytes := size
		usage = []*csi.VolumeUsage{
			{Unit: csi.VolumeUsage_BYTES, Total: totalBytes, Available: totalBytes, Used: 0},
		}
	}

	klog.V(4).Infof("NodeGetVolumeStats for volume %s at %s completed (condition: abnormal=%v, message=%s)", volumeID, volumePath, condition.Abnormal, condition.Message)
	return &csi.NodeGetVolumeStatsResponse{
		Usage:           usage,
		VolumeCondition: condition,
	}, nil
}
