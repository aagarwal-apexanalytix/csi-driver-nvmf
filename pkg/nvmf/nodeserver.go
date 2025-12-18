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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
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
	// Pre-check
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

	// Force cleanup of any existing connection/persistence for this volume
	connectorFilePath := path.Join(DefaultVolumeMapPath, req.GetVolumeId()+".json")
	if _, err := os.Stat(connectorFilePath); err == nil {
		if connector, loadErr := GetConnectorFromFile(connectorFilePath); loadErr == nil {
			klog.Infof("Force disconnecting existing connection for volume %s before new publish", req.VolumeId)
			_ = connector.Disconnect()
		}
		removeConnectorFile(connectorFilePath)
	}

	// Connect remote disk (always fresh connect - no idempotent skip)
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

	// Persist new connector
	err = persistConnectorFile(connector, connectorFilePath)
	if err != nil {
		klog.Errorf("failed to persist connection info: %v", err)
		// Rollback connect
		_ = connector.Disconnect()
		removeConnectorFile(connectorFilePath)
		return nil, status.Errorf(codes.Internal, "VolumeID %s persist connection info error: %v", req.VolumeId, err)
	}

	// Attach disk to container path
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

	// Detach disk (unmount)
	targetPath := req.GetTargetPath()
	err := DetachDisk(targetPath)
	if err != nil {
		klog.Errorf("VolumeID: %s detachDisk err: %v", req.VolumeId, err)
		return nil, err
	}

	// Force disconnect the NVMe-oF subsystem to reset kernel state
	// First try from persistence file
	connectorFilePath := path.Join(DefaultVolumeMapPath, req.GetVolumeId()+".json")
	var nqn string
	if connector, loadErr := GetConnectorFromFile(connectorFilePath); loadErr == nil {
		nqn = connector.TargetNqn
		_ = connector.Disconnect()
	} else {
		// Fallback: assume NQN == VolumeID
		nqn = req.VolumeId
	}

	// Force disconnect by NQN (ignores hostnqn for broad cleanup)
	ret := disconnectByNqn(nqn, "")
	if ret > 0 {
		klog.Infof("Forced disconnect of %d controllers for NQN %s", ret, nqn)
	} else if ret < 0 {
		klog.Warningf("Force disconnect failed for NQN %s", nqn)
	}

	// Clean persistence
	removeConnectorFile(connectorFilePath)

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
	controller, _, err := getNvmeInfoByNqn(req.VolumeId)
	if err != nil {
		klog.Errorf("NodeExpandVolume: failed to find controller for %s: %v", req.VolumeId, err)
		return nil, status.Errorf(codes.Internal, "failed to find controller for volume %s: %v", req.VolumeId, err)
	}

	scanPath := filepath.Join("/sys/class/nvme-fabrics/ctl", controller, "rescan_controller")
	klog.Infof("Triggering NVMe-oF controller rescan at %s for volume %s", scanPath, req.VolumeId)

	if !utils.IsFileExisting(scanPath) {
		return nil, status.Errorf(codes.Internal, "NodeExpandVolume: rescan path %s not exist", scanPath)
	}

	file, err := os.OpenFile(scanPath, os.O_WRONLY, 0666)
	if err != nil {
		klog.Errorf("NodeExpandVolume: open scan path %s error: %v", scanPath, err)
		return nil, status.Errorf(codes.Internal, "NodeExpandVolume: open scan path %s error: %v", scanPath, err)
	}
	defer file.Close()

	if _, err = file.WriteString("1"); err != nil {
		klog.Errorf("NodeExpandVolume: Rescan write error: %v", err)
		return nil, status.Errorf(codes.Internal, "NodeExpandVolume: Rescan write error: %v", err)
	}

	klog.Infof("Successfully triggered controller rescan for volume %s", req.VolumeId)
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (n *NodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: n.Driver.nodeId,
	}, nil
}

func (n *NodeServer) NodeGetVolumeStats(_ context.Context, _ *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "NodeGetVolumeStats not implement")
}
