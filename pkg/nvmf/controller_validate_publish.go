// Package nvmf controller_validate_publish.go
package nvmf

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func (cs *ControllerServer) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}

	supported := true
	for _, capability := range req.GetVolumeCapabilities() {
		if capability.GetBlock() == nil && capability.GetMount() == nil {
			supported = false
			continue
		}

		mode := capability.GetAccessMode().GetMode()
		if capability.GetBlock() != nil {
			if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				supported = false
			}
		} else {
			if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				supported = false
			}
		}
	}

	if supported {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
				VolumeCapabilities: req.GetVolumeCapabilities(),
			},
		}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(_ context.Context, _ *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(5).Infof("ControllerPublishVolume called (no-op for NVMe-oF)")
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerUnpublishVolume(_ context.Context, _ *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(5).Infof("ControllerUnpublishVolume called (no-op for NVMe-oF)")
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}
