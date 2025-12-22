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
	klog.Infof("Using Nvme NodeGetCapabilities")
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
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

func (n *NodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Pre-checks
	if req.GetVolumeCapability() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing Volume Capability")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing VolumeID")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing TargetPath")
	}

	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	klog.Infof("NodePublishVolume: VolumeID %s publish to targetPath %s.", volumeID, targetPath)

	klog.V(4).Infof("NodePublishVolume: NVMe mode for volume %s", volumeID)

	// Connect remote disk (idempotent if already connected with same parameters)
	nvmfInfo, err := getNVMfDiskInfo(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodePublishVolume: get NVMf disk info from req err: %v", err)
	}

	connector := getNvmfConnector(nvmfInfo)
	devicePath, err := connector.Connect()
	if err != nil {
		klog.Errorf("VolumeID %s failed to connect, Error: %v", volumeID, err)
		return nil, status.Errorf(codes.Internal, "VolumeID %s failed to connect, Error: %v", volumeID, err)
	}
	if devicePath == "" {
		klog.Errorf("VolumeID %s connected, but return nil devicePath", volumeID)
		return nil, status.Errorf(codes.Internal, "VolumeID %s connected, but return nil devicePath", volumeID)
	}
	klog.Infof("Volume %s successful connected, Device: %s", volumeID, devicePath)

	// Persist new/updated connector (overwrites if exists)
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	err = persistConnectorFile(connector, connectorFilePath)
	if err != nil {
		klog.Errorf("failed to persist connection info: %v", err)
		// Rollback connect
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "VolumeID %s persist connection info error: %v", volumeID, err)
	}

	// Validate access type
	if req.GetVolumeCapability().GetBlock() != nil && req.GetVolumeCapability().GetMount() != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot have both block and mount access type")
	}

	err = AttachDisk(req, devicePath)
	if err != nil {
		// Rollback on attach failure
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "VolumeID %s attach error: %v", volumeID, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume: Starting unpublish volume %s at %s", req.VolumeId, req.TargetPath)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume VolumeID must be provided")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume TargetPath must be provided")
	}

	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	// Always cleanup the target mount point
	if err := mount.CleanupMountPoint(targetPath, n.mounter, true); err != nil {
		klog.Errorf("Failed to cleanup mount point %s: %v", targetPath, err)
		return nil, status.Errorf(codes.Internal, "Failed to unpublish %s: %v", targetPath, err)
	}

	// Disconnect NVMe subsystem if connector file exists
	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
	if connector, err := GetConnectorFromFile(connectorFilePath); err == nil {
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		klog.V(4).Infof("Disconnected NVMe subsystem for volume %s", volumeID)
	}

	klog.Infof("NodeUnpublishVolume completed for volume %s", req.VolumeId)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeStageVolume(_ context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnstageVolume(_ context.Context, _ *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *NodeServer) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumeID := req.VolumeId
	klog.Infof("NodeExpandVolume called for volume %s with required bytes %d", volumeID, req.CapacityRange.GetRequiredBytes())

	connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")

	var devicePath string
	var err error

	// Primary attempt: find existing device
	_, devicePath, err = getNvmeInfoByNqn(volumeID)
	if err == nil && devicePath != "" {
		klog.Infof("Found existing device %s for volume %s", devicePath, volumeID)
	} else {
		klog.Warningf("Initial device lookup failed for volume %s: %v — attempting recovery via persisted connector", volumeID, err)

		// Recovery: load persisted connector and reconnect
		connector, loadErr := GetConnectorFromFile(connectorFilePath)
		if loadErr != nil {
			klog.Errorf("Failed to load persisted connector for recovery: %v", loadErr)
			return nil, status.Errorf(codes.Internal, "failed to find device for volume %s and recovery failed: %v", volumeID, err)
		}

		recoveredDevice, reconErr := connector.Connect()
		if reconErr != nil {
			klog.Errorf("Recovery reconnect failed for volume %s: %v", volumeID, reconErr)
			return nil, status.Errorf(codes.Internal, "failed to recover connection for volume %s: %v", volumeID, reconErr)
		}
		if recoveredDevice == "" {
			klog.Errorf("Recovery reconnect succeeded but returned empty device path for volume %s", volumeID)
			return nil, status.Errorf(codes.Internal, "recovery reconnect for volume %s returned empty device path", volumeID)
		}

		// Wait for device to appear
		maxRetries := 30
		for i := 0; i < maxRetries; i++ {
			_, devicePath, err = getNvmeInfoByNqn(volumeID)
			if err == nil && devicePath != "" {
				klog.Infof("Recovery successful: device %s now available for volume %s", devicePath, volumeID)
				break
			}
			time.Sleep(2 * time.Second)
		}
		if devicePath == "" {
			klog.Errorf("Recovery failed: device still not found after reconnect for volume %s", volumeID)
			return nil, status.Errorf(codes.Internal, "failed to recover device for volume %s after reconnect", volumeID)
		}
	}

	// Rescan controllers (robust)
	const ctlPath = "/sys/class/nvme-fabrics/ctl"
	entries, err := os.ReadDir(ctlPath)
	if err != nil {
		klog.Errorf("Failed to read %s during rescan: %v", ctlPath, err)
	} else {
		rescanTriggered := false
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
				klog.Infof("Triggering rescan at %s for volume %s", scanPath, volumeID)
				if utils.IsFileExisting(scanPath) {
					file, err := os.OpenFile(scanPath, os.O_WRONLY, 0666)
					if err != nil {
						klog.Errorf("Failed to open rescan path %s: %v", scanPath, err)
					} else {
						defer file.Close()
						if _, err = file.WriteString("1"); err != nil {
							klog.Errorf("Failed to trigger rescan on %s: %v", entry.Name(), err)
						} else {
							klog.Infof("Successfully triggered rescan for controller %s", entry.Name())
							rescanTriggered = true
						}
					}
				}
			}
		}
		if !rescanTriggered {
			klog.Warningf("No rescan paths found for volume %s (may already be updated)", volumeID)
		}
	}

	// Online filesystem resize (assumes ext4)
	klog.Infof("Running online resize2fs on %s for volume %s", devicePath, volumeID)
	out, err := exec.New().Command("resize2fs", devicePath).CombinedOutput()
	if err != nil {
		klog.Errorf("resize2fs failed for %s: %v\nOutput: %s", devicePath, err, string(out))
		return nil, status.Errorf(codes.Internal, "filesystem resize failed: %v\nOutput: %s", err, string(out))
	}
	klog.Infof("resize2fs success: %s", string(out))

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: req.CapacityRange.GetRequiredBytes(),
	}, nil
}

func (n *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: n.Driver.nodeId,
	}, nil
}

func (n *NodeServer) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path missing in request")
	}

	volumePath := req.VolumePath

	stat, err := os.Stat(volumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "Volume path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "Failed to stat volume path %s: %v", volumePath, err)
	}

	if !stat.IsDir() {
		klog.V(4).Infof("NodeGetVolumeStats: Volume %s is raw block (file), stats not supported", req.VolumeId)
		return nil, status.Errorf(codes.Unimplemented, "NodeGetVolumeStats not supported for raw block volumes")
	}

	var statfs syscall.Statfs_t
	if err := syscall.Statfs(volumePath, &statfs); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get filesystem stats for %s: %v", volumePath, err)
	}

	totalBytes := int64(statfs.Blocks * uint64(statfs.Bsize))
	availableBytes := int64(statfs.Bfree * uint64(statfs.Bsize))
	usedBytes := totalBytes - availableBytes
	totalInodes := int64(statfs.Files)
	freeInodes := int64(statfs.Ffree)
	usedInodes := totalInodes - freeInodes

	klog.V(4).Infof("NodeGetVolumeStats for volume %s at %s: total=%d, available=%d, used=%d bytes; inodes total=%d, used=%d", req.VolumeId, volumePath, totalBytes, availableBytes, usedBytes, totalInodes, usedInodes)

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Total:     totalBytes,
				Available: availableBytes,
				Used:      usedBytes,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Total:     totalInodes,
				Available: freeInodes,
				Used:      usedInodes,
			},
		},
	}, nil
}
