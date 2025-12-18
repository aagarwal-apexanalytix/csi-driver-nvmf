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
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/utils/exec"
)

type NodeServer struct {
	Driver *driver
}

func NewNodeServer(d *driver) *NodeServer {
	return &NodeServer{
		Driver: d,
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
		},
	}, nil
}

func (n *NodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Pre-checks
	if req.GetVolumeCapability() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing Volume Capability in req.")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing VolumeID in req.")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "NodePublishVolume missing TargetPath in req.")
	}

	klog.Infof("VolumeID %s publish to targetPath %s.", req.GetVolumeId(), req.GetTargetPath())

	// Connect remote disk (idempotent if already connected with same parameters)
	nvmfInfo, err := getNVMfDiskInfo(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodePublishVolume: get NVMf disk info from req err: %v", err)
	}
	connector := getNvmfConnector(nvmfInfo)
	devicePath, err := connector.Connect()
	if err != nil {
		klog.Errorf("VolumeID %s failed to connect, Error: %v", req.VolumeId, err)
		return nil, status.Errorf(codes.Internal, "VolumeID %s failed to connect, Error: %v", req.VolumeId, err)
	}
	if devicePath == "" {
		klog.Errorf("VolumeID %s connected, but return nil devicePath", req.VolumeId)
		return nil, status.Errorf(codes.Internal, "VolumeID %s connected, but return nil devicePath", req.VolumeId)
	}
	klog.Infof("Volume %s successful connected, Device: %s", req.VolumeId, devicePath)

	// Persist new/updated connector (overwrites if exists)
	connectorFilePath := path.Join(DefaultVolumeMapPath, req.GetVolumeId()+".json")
	err = persistConnectorFile(connector, connectorFilePath)
	if err != nil {
		klog.Errorf("failed to persist connection info: %v", err)
		// Rollback connect
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "VolumeID %s persist connection info error: %v", req.VolumeId, err)
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
		return nil, status.Errorf(codes.Internal, "VolumeID %s attach error: %v", req.VolumeId, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume: Starting unpublish volume, %s, %v", req.VolumeId, req)

	// Pre-check
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume VolumeID must be provided")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume TargetPath must be provided")
	}

	// Detach disk (unmount only)
	targetPath := req.GetTargetPath()
	err := DetachDisk(targetPath)
	if err != nil {
		klog.Errorf("VolumeID: %s detachDisk err: %v", req.VolumeId, err)
		return nil, err
	}

	klog.Infof("NodeUnpublishVolume completed for volume %s (connection and persistence preserved)", req.VolumeId)
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

	var devicePath string
	var err error

	// Primary attempt: find existing controller/device
	_, devicePath, err = getNvmeInfoByNqn(volumeID)
	if err == nil && devicePath != "" {
		klog.Infof("Found existing device %s for volume %s", devicePath, volumeID)
	} else {
		klog.Warningf("Initial device lookup failed for volume %s: %v — attempting recovery via persisted connector", volumeID, err)

		// Recovery: load persisted connector and reconnect (idempotent)
		connectorFilePath := path.Join(DefaultVolumeMapPath, volumeID+".json")
		connector, loadErr := GetConnectorFromFile(connectorFilePath)
		if loadErr != nil {
			klog.Errorf("Failed to load persisted connector for recovery: %v", loadErr)
			return nil, status.Errorf(codes.Internal, "failed to find device for volume %s and recovery failed: %v", volumeID, err)
		}

		// Reconnect (safe if already connected)
		recoveredDevice, reconErr := connector.Connect()
		if reconErr != nil {
			klog.Errorf("Recovery reconnect failed for volume %s: %v", volumeID, reconErr)
			return nil, status.Errorf(codes.Internal, "failed to recover connection for volume %s: %v", volumeID, reconErr)
		}
		if recoveredDevice == "" {
			klog.Errorf("Recovery reconnect succeeded but returned empty device path for volume %s", volumeID)
			return nil, status.Errorf(codes.Internal, "recovery reconnect for volume %s returned empty device path", volumeID)
		}

		// Wait for device to appear (reuse existing wait logic or simple sleep + retry)
		maxRetries := 30 // ~60s total
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

	// Rescan ALL controllers matching the NQN (robust)
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

	// Online filesystem resize
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

func (n *NodeServer) NodeGetVolumeStats(_ context.Context, _ *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "NodeGetVolumeStats not implement")
}
