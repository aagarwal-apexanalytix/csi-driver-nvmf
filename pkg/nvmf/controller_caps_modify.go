// controller_caps_modify.go
package nvmf

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(5).Infof("ControllerGetCapabilities requested")

	var caps []*csi.ControllerServiceCapability
	add := func(cap csi.ControllerServiceCapability_RPC_Type) {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: cap},
			},
		})
	}

	add(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME)
	add(csi.ControllerServiceCapability_RPC_LIST_VOLUMES)
	add(csi.ControllerServiceCapability_RPC_GET_CAPACITY)
	add(csi.ControllerServiceCapability_RPC_GET_VOLUME)
	add(csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)

	if cs.provider != ProviderStatic {
		add(csi.ControllerServiceCapability_RPC_CLONE_VOLUME)
		add(csi.ControllerServiceCapability_RPC_MODIFY_VOLUME)
		add(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME)
		add(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT)
		add(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS)
		add(csi.ControllerServiceCapability_RPC_GET_SNAPSHOT)
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (cs *ControllerServer) ControllerModifyVolume(_ context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	cs.initConfig()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not supported in static provider mode")
	}

	mutableParams := req.GetMutableParameters()
	if len(mutableParams) == 0 {
		klog.V(4).Infof("ControllerModifyVolume no-op success for volume %s (no mutable parameters provided)", volumeID)
		return &csi.ControllerModifyVolumeResponse{}, nil
	}
	klog.V(4).Infof("ControllerModifyVolume requested for volume %s with mutable parameters: %v", volumeID, mutableParams)

	// NOTE: global creds (request doesn't include VolumeContext)
	restURL := cs.restURL
	username := cs.username
	password := cs.password

	if cs.provider == ProviderNvmeProxy {
		// nvme-proxy can patch by slot
		if v, ok := mutableParams["comment"]; ok {
			if err := cs.restPatch("/disk/"+volumeID, map[string]string{"comment": v}, restURL, username, password); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to update comment on volume %s: %v", volumeID, err)
			}
			return &csi.ControllerModifyVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.InvalidArgument, "Unsupported mutable parameters %v (only 'comment' supported)", mutableParams)
	}

	diskID, err := cs.getDiskID(volumeID, restURL, username, password)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Failed to locate disk for volume %s: %v", volumeID, err)
	}

	for key, value := range mutableParams {
		switch key {
		case "comment":
			if err := cs.restPost("/disk/set", map[string]string{
				"numbers": diskID,
				"comment": value,
			}, restURL, username, password); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to update comment on disk %s: %v", diskID, err)
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported mutable parameter %q (only 'comment' supported)", key)
		}
	}

	return &csi.ControllerModifyVolumeResponse{}, nil
}
