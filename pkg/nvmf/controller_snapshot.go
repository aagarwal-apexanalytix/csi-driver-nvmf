package nvmf

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

func (cs *ControllerServer) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID is required")
	}
	listResp, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: req.GetSnapshotId()})
	if err != nil {
		return nil, err
	}
	if len(listResp.GetEntries()) == 0 {
		return nil, status.Error(codes.NotFound, "Snapshot not found")
	}
	if len(listResp.GetEntries()) > 1 {
		klog.Warningf("GetSnapshot found multiple entries for ID %s", req.GetSnapshotId())
	}
	return &csi.GetSnapshotResponse{Snapshot: listResp.Entries[0].Snapshot}, nil
}

func (cs *ControllerServer) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	cs.initConfig()
	sourceVol := req.GetSourceVolumeId()
	snapName := req.GetName()
	if sourceVol == "" || snapName == "" {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId and Name required")
	}
	klog.V(4).Infof("CreateSnapshot requested: name=%s, sourceVolumeId=%s, parameters=%v", snapName, sourceVol, req.GetParameters())

	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "Snapshots not supported in static mode")
	}
	// If nvme-proxy backend is zfs, snapshots should be implemented in nvme-proxy API (not BTRFS subvol).
	// For now, keep BTRFS subvol snapshots only.
	if cs.provider == ProviderNvmeProxy && strings.EqualFold(cs.backendMount, "zfs") {
		return nil, status.Error(codes.Unimplemented, "Snapshots not implemented for nvme-proxy zfs backend")
	}

	params := req.GetParameters()
	localRestURL := params["restURL"]
	if localRestURL == "" {
		localRestURL = cs.restURL
	}
	if localRestURL == "" {
		return nil, status.Error(codes.InvalidArgument, "restURL required (no global configured)")
	}

	localUsername := params["username"]
	if localUsername == "" {
		localUsername = cs.username
	}
	localPassword := params["password"]
	if localPassword == "" {
		localPassword = cs.password
	}

	if cs.provider != ProviderNvmeProxy && cs.provider != ProviderStatic {
		if localUsername == "" || localPassword == "" {
			return nil, status.Error(codes.InvalidArgument, "username/password required (no global configured)")
		}
	}

	backendDisk := params["backendDisk"]
	if backendDisk == "" {
		backendDisk = cs.backendDisk
	}

	sourceSubVol := "vol-" + sourceVol
	snapSubVol := "snap-" + snapName + "-" + sourceVol

	snapData := map[string]string{
		"fs":        backendDisk,
		"name":      snapSubVol,
		"parent":    sourceSubVol,
		"read-only": "yes",
	}
	if err := cs.restPost("/disk/btrfs/subvolume/add", snapData, localRestURL, localUsername, localPassword); err != nil {
		return nil, status.Errorf(codes.Internal, "Snapshot create failed: %v", err)
	}

	_, sourceSize, err := cs.volumeExists(sourceVol, localRestURL, localUsername, localPassword)
	if err != nil {
		klog.Warningf("Failed to retrieve source volume size for snapshot %s: %v", snapSubVol, err)
		sourceSize = 0
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapSubVol,
			SourceVolumeId: sourceVol,
			CreationTime:   timestamppb.New(time.Now()),
			ReadyToUse:     true,
			SizeBytes:      sourceSize,
		},
	}, nil
}

func (cs *ControllerServer) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	cs.initConfig()
	snapSubVol := req.GetSnapshotId()
	if snapSubVol == "" {
		return nil, status.Error(codes.InvalidArgument, "SnapshotId required")
	}
	klog.V(4).Infof("DeleteSnapshot requested for snapshot %s", snapSubVol)

	if cs.provider == ProviderStatic {
		return &csi.DeleteSnapshotResponse{}, nil
	}

	_ = cs.restDelete("/disk/btrfs/subvolume/"+snapSubVol, cs.restURL, cs.username, cs.password)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	cs.initConfig()
	if cs.provider == ProviderStatic {
		return &csi.ListSnapshotsResponse{}, nil
	}
	// For nvme-proxy zfs, not implemented here.
	if cs.provider == ProviderNvmeProxy && strings.EqualFold(cs.backendMount, "zfs") {
		return &csi.ListSnapshotsResponse{}, nil
	}

	subvols, err := cs.restGet("/disk/btrfs/subvolume", cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list BTRFS subvolumes: %v", err)
	}

	var snaps []*csi.Snapshot
	for _, sv := range subvols {
		nameIfc, ok := sv["name"]
		if !ok {
			continue
		}
		name, _ := nameIfc.(string)
		if !strings.HasPrefix(name, "snap-") {
			continue
		}

		ro := ""
		if v, ok := sv["read-only"].(string); ok {
			ro = v
		}
		if ro != "yes" {
			continue
		}

		parent, _ := sv["parent"].(string)
		if !strings.HasPrefix(parent, "vol-") {
			continue
		}
		sourceVol := strings.TrimPrefix(parent, "vol-")

		if req.GetSnapshotId() != "" && req.GetSnapshotId() != name {
			continue
		}
		if req.GetSourceVolumeId() != "" && req.GetSourceVolumeId() != sourceVol {
			continue
		}

		_, sourceSize, err := cs.volumeExists(sourceVol, cs.restURL, cs.username, cs.password)
		if err != nil {
			klog.Warningf("Failed to get size for source volume %s of snapshot %s: %v", sourceVol, name, err)
			sourceSize = 0
		}

		snaps = append(snaps, &csi.Snapshot{
			SnapshotId:     name,
			SourceVolumeId: sourceVol,
			ReadyToUse:     true,
			SizeBytes:      sourceSize,
		})
	}

	// Pagination
	startIdx := 0
	if token := req.GetStartingToken(); token != "" {
		if i, err := strconv.Atoi(token); err == nil && i >= 0 && i < len(snaps) {
			startIdx = i
		}
	}

	maxEntries := 0
	if req.GetMaxEntries() > 0 {
		maxEntries = int(req.GetMaxEntries())
	}

	endIdx := len(snaps)
	if maxEntries > 0 && startIdx+maxEntries < endIdx {
		endIdx = startIdx + maxEntries
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: snaps[i]})
	}

	nextToken := ""
	if endIdx < len(snaps) {
		nextToken = strconv.Itoa(endIdx)
	}

	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}
