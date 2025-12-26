// Package nvmf - ControllerServer volume lifecycle (Create/Delete/List/Expand/GetCapacity/GetVolume)
//
// Updates for nvme-proxy disk model changes:
// - nvme-proxy CreateDiskRequest.NvmeTcpExport is now *bool (JSON boolean) and NvmeTcpPort is *int
// - nvme-proxy DiskResponse now returns nvmeTcpExport/nvmeTcpDesired as bool and nvmeTcpPort as int
// - DO NOT call BTRFS subvolume APIs for nvme-proxy (MikroTik only)
//
// Additional update:
// - For nvme-proxy provider, fetch NQN from nvme-proxy (GET /disk/:slot) instead of assuming nqn == volumeID.
//
// You still need to wire your rest helpers (restGet/restPost/restPatch/restDelete),
// config/initConfig, and any snapshot/clone helpers you already have.
package nvmf

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func (cs *ControllerServer) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	cs.initConfig()

	klog.V(4).Infof(
		"CreateVolume requested: name=%s, capacityRange=%v, parameters=%v, contentSource=%v",
		req.GetName(), req.CapacityRange, req.Parameters, req.VolumeContentSource,
	)

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name required")
	}

	params := req.GetParameters()

	// fstype (use new param; fallback to deprecated "fstype")
	fstype := strings.TrimSpace(params["csi.storage.k8s.io/fstype"])
	if fstype == "" {
		fstype = strings.TrimSpace(params["fstype"]) // deprecated but tolerated
	}
	if fstype == "" {
		fstype = "ext4"
	}
	if fstype != "ext4" && fstype != "xfs" {
		klog.Warningf("Requested fstype %q is not explicitly supported by online resize logic (supported: ext4, xfs)", fstype)
	}

	// NVMe target required for dynamic
	targetAddr := strings.TrimSpace(params["targetAddr"])
	if targetAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "targetAddr is required in StorageClass parameters")
	}
	targetPortStr := strings.TrimSpace(params["targetPort"])
	if targetPortStr == "" {
		targetPortStr = defaultTargetPort
	}
	targetPortInt, err := strconv.Atoi(targetPortStr)
	if err != nil || targetPortInt <= 0 || targetPortInt > 65535 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid targetPort %q (must be 1-65535)", targetPortStr)
	}

	// backend config (optional per-SC overrides)
	backendDisk := strings.TrimSpace(params["backendDisk"])
	if backendDisk == "" {
		backendDisk = cs.backendDisk
	}
	backendMount := strings.TrimSpace(params["backendMount"])
	if backendMount == "" {
		backendMount = cs.backendMount
	}

	// CSI metadata (best-effort)
	pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]
	pvcHumanName := params["csi.storage.k8s.io/pvc/name"]
	pvName := params["csi.storage.k8s.io/pv/name"]
	if pvcNamespace == "" {
		klog.Warningf("PVC namespace param missing - ensure csi-provisioner has --extra-create-metadata=true")
		pvcNamespace = "unknown"
	}
	if pvcHumanName == "" {
		klog.Warningf("PVC name param missing - falling back to req.Name")
		pvcHumanName = req.GetName()
	}
	if pvName == "" {
		klog.Warningf("PV name param missing - falling back to req.Name")
		pvName = req.GetName()
	}
	clusterName := params["clusterName"]
	if clusterName == "" {
		clusterName = "unknown"
	}

	// per-SC endpoint/creds
	localRestURL := strings.TrimSpace(params["restURL"])
	if localRestURL == "" {
		localRestURL = cs.restURL
	}
	if localRestURL == "" {
		return nil, status.Error(codes.InvalidArgument, "restURL required (no global configured)")
	}
	localUsername := strings.TrimSpace(params["username"])
	if localUsername == "" {
		localUsername = cs.username
	}
	localPassword := strings.TrimSpace(params["password"])
	if localPassword == "" {
		localPassword = cs.password
	}

	// nvme-proxy often runs with no auth; static doesn't require auth
	if cs.provider != ProviderNvmeProxy && cs.provider != ProviderStatic {
		if localUsername == "" {
			return nil, status.Error(codes.InvalidArgument, "username required (no global configured)")
		}
		if localPassword == "" {
			return nil, status.Error(codes.InvalidArgument, "password required (no global configured)")
		}
	}

	// cloning / restore
	// NOTE: for nvme-proxy, do NOT use BTRFS subvol APIs.
	// Content source support for nvme-proxy should be implemented using nvme-proxy snapshot/clone endpoints (if/when wired).
	contentSource := req.GetVolumeContentSource()
	var parentSubvol string
	var sourceCapacity int64
	var cloneInfo string
	if contentSource != nil {
		if cs.provider == ProviderStatic {
			return nil, status.Error(codes.Unimplemented, "Volume cloning/from-snapshot not supported in static mode")
		}
		if cs.provider == ProviderNvmeProxy {
			return nil, status.Error(codes.Unimplemented, "ContentSource (clone/restore) not implemented for nvme-proxy in this controller path")
		}
		var err error
		parentSubvol, sourceCapacity, cloneInfo, err = cs.handleContentSource(contentSource, localRestURL, localUsername, localPassword, &fstype)
		if err != nil {
			return nil, err
		}
	}

	comment := fmt.Sprintf(
		"k8s-pvc:%s:%s:%s/%s (pv:%s) fstype:%s%s",
		clusterName, backendMount, pvcNamespace, pvcHumanName, pvName, fstype, cloneInfo,
	)

	// IDs
	volumeID := strings.ReplaceAll(strings.ToLower(pvName), "-", "")
	slot := volumeID
	subVolName := "vol-" + volumeID

	// MikroTik legacy file path under mount (only used in MikroTik provider branch)
	imgPath := strings.TrimRight(backendMount, "/") + "/" + subVolName + "/volume.img"

	// capacity
	requiredBytes := int64(0)
	if req.GetCapacityRange() != nil {
		requiredBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	var effectiveBytes int64
	if contentSource != nil {
		if requiredBytes > 0 && requiredBytes < sourceCapacity {
			return nil, status.Errorf(codes.InvalidArgument, "Requested capacity %d bytes is less than source capacity %d bytes", requiredBytes, sourceCapacity)
		}
		effectiveBytes = sourceCapacity
		if requiredBytes > sourceCapacity {
			effectiveBytes = requiredBytes
		}
	} else {
		effectiveBytes = requiredBytes
		if effectiveBytes == 0 || effectiveBytes < minVolumeSize {
			effectiveBytes = minVolumeSize
		}
	}

	// round up to GiB
	giBFloat := math.Ceil(float64(effectiveBytes) / (1024.0 * 1024.0 * 1024.0))
	sizeGiB := fmt.Sprintf("%dG", int64(giBFloat))
	actualBytes := int64(giBFloat) * 1024 * 1024 * 1024

	// STATIC mode: must pre-exist
	if cs.provider == ProviderStatic {
		exists, currentSize, _ := cs.volumeExists(volumeID, cs.restURL, cs.username, cs.password)
		if !exists {
			return nil, status.Error(codes.NotFound, "Volume not pre-created in static mode")
		}
		if currentSize < actualBytes {
			return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested (static mode)")
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: currentSize,
				VolumeContext: map[string]string{
					"targetTrAddr":              targetAddr,
					"targetTrPort":              targetPortStr,
					"targetTrType":              "tcp",
					"nqn":                       slot,
					"deviceID":                  slot,
					"csi.storage.k8s.io/fstype": fstype,
				},
			},
		}, nil
	}

	// Idempotency (dynamic)
	exists, currentSize, err := cs.volumeExists(volumeID, localRestURL, localUsername, localPassword)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to check existing volume: %v", err)
	}

	if exists {
		if currentSize >= actualBytes {
			// best-effort comment update
			if cs.provider == ProviderNvmeProxy {
				_ = cs.restPatch("/disk/"+slot, map[string]any{"comment": comment}, localRestURL, localUsername, localPassword)
			} else {
				if diskID, getErr := cs.getDiskID(slot, localRestURL, localUsername, localPassword); getErr == nil && diskID != "" {
					_ = cs.restPost("/disk/set", map[string]any{"numbers": diskID, "comment": comment}, localRestURL, localUsername, localPassword)
				}
			}

			// IMPORTANT: fetch NQN from nvme-proxy if applicable
			nqn := slot
			if cs.provider == ProviderNvmeProxy {
				nqn = cs.getNvmeProxyNqn(slot, localRestURL, localUsername, localPassword)
			}

			volCtx := map[string]string{
				"targetTrAddr":              targetAddr,
				"targetTrPort":              targetPortStr,
				"targetTrType":              "tcp",
				"nqn":                       nqn,
				"deviceID":                  slot,
				"csi.storage.k8s.io/fstype": fstype,
			}

			if cs.provider == ProviderNvmeProxy {
				if v := nvmeProxyCompressFromParams(params); v != "" {
					volCtx["compression"] = v
				}
			} else {
				if v := getEffectiveCompress(params); v != "" {
					volCtx["compression"] = v
				}
			}

			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      volumeID,
					CapacityBytes: currentSize,
					VolumeContext: volCtx,
					ContentSource: contentSource,
				},
			}, nil
		}
		return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested")
	}

	klog.V(4).Infof("Provisioning new volume %s (slot=%s, size=%s)%s", req.GetName(), slot, sizeGiB, cloneInfo)

	var compressForContext string

	// Provider-specific disk create + export
	switch cs.provider {
	case ProviderNvmeProxy:
		// nvme-proxy: POST /disk/add
		// nvmeTcpExport: *bool, nvmeTcpPort: *int
		enable := true
		port := targetPortInt

		diskData := map[string]any{
			"slot":          slot,
			"fileSize":      sizeGiB,
			"comment":       comment,
			"nvmeTcpExport": &enable, // pointer-to-bool matches *bool
			"nvmeTcpPort":   &port,   // pointer-to-int matches *int
		}

		if c := nvmeProxyCompressFromParams(params); c != "" {
			diskData["compress"] = c
			compressForContext = c
		}

		if err := cs.restPost("/disk/add", diskData, localRestURL, localUsername, localPassword); err != nil {
			// rollback best-effort
			_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
			return nil, status.Errorf(codes.Internal, "nvme-proxy disk create failed: %v", err)
		}

		// Best-effort: ensure export enabled (nvme-proxy PATCH expects bool, port expects int)
		_ = cs.restPatch("/disk/"+slot, map[string]any{
			"nvmeTcpExport": true,
			"nvmeTcpPort":   targetPortInt,
		}, localRestURL, localUsername, localPassword)

	default:
		// MikroTik style (or anything else): create BTRFS subvolume, create file-backed disk, export, compress/comment via /disk/set

		// Create BTRFS subvolume (MikroTik only)
		subvolData := map[string]any{"fs": backendDisk, "name": subVolName}
		if parentSubvol != "" {
			subvolData["parent"] = parentSubvol
			subvolData["read-only"] = "no"
		}
		if err := cs.restPost("/disk/btrfs/subvolume/add", subvolData, localRestURL, localUsername, localPassword); err != nil {
			if strings.Contains(err.Error(), "exists") || strings.Contains(err.Error(), "File exists") {
				klog.V(4).Infof("Subvolume %s already exists (idempotent)", subVolName)
			} else {
				return nil, status.Errorf(codes.Internal, "Subvolume create failed: %v", err)
			}
		}

		diskData := map[string]any{
			"type":      "file",
			"file-path": imgPath,
			"file-size": sizeGiB,
			"slot":      slot,
		}
		if err := cs.restPost("/disk/add", diskData, localRestURL, localUsername, localPassword); err != nil {
			_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			return nil, status.Errorf(codes.Internal, "Disk create failed: %v", err)
		}

		diskID, err := cs.getDiskID(slot, localRestURL, localUsername, localPassword)
		if err != nil || diskID == "" {
			_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
			_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			return nil, status.Errorf(codes.Internal, "Failed to retrieve disk .id for export: %v", err)
		}

		// Enable export
		if err := cs.restPatch("/disk/"+diskID, map[string]any{
			"nvme-tcp-export": "yes",
			"nvme-tcp-port":   targetPortStr,
		}, localRestURL, localUsername, localPassword); err != nil {
			klog.Warningf("PATCH export failed, trying SET: %v", err)
			if err2 := cs.restPost("/disk/set", map[string]any{
				"numbers":         diskID,
				"nvme-tcp-export": "yes",
				"nvme-tcp-port":   targetPortStr,
			}, localRestURL, localUsername, localPassword); err2 != nil {
				_ = cs.restDelete("/disk/"+diskID, localRestURL, localUsername, localPassword)
				_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
				return nil, status.Errorf(codes.Internal, "Export enable failed: %v / %v", err, err2)
			}
		}

		// Compression for MikroTik: yes/no
		compressValue := getEffectiveCompress(params)
		compressForContext = compressValue
		if compressValue != "" {
			if err := cs.restPost("/disk/set", map[string]any{
				"numbers":  diskID,
				"compress": compressValue,
			}, localRestURL, localUsername, localPassword); err != nil {
				klog.Warningf("Failed to set compress=%s on disk %s: %v (continuing anyway)", compressValue, diskID, err)
			}
		}

		// Comment
		_ = cs.restPost("/disk/set", map[string]any{"numbers": diskID, "comment": comment}, localRestURL, localUsername, localPassword)
	}

	// IMPORTANT: fetch NQN from nvme-proxy if applicable
	nqn := slot
	if cs.provider == ProviderNvmeProxy {
		nqn = cs.getNvmeProxyNqn(slot, localRestURL, localUsername, localPassword)
	}

	klog.V(4).Infof("Successfully created volume %s (actual size %d bytes)%s", volumeID, actualBytes, cloneInfo)

	volCtx := map[string]string{
		"targetTrAddr":              targetAddr,
		"targetTrPort":              targetPortStr,
		"targetTrType":              "tcp",
		"nqn":                       nqn,
		"deviceID":                  slot,
		"csi.storage.k8s.io/fstype": fstype,
	}
	if compressForContext != "" {
		volCtx["compression"] = compressForContext
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: actualBytes,
			VolumeContext: volCtx,
			ContentSource: contentSource,
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	cs.initConfig()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}

	klog.V(4).Infof("DeleteVolume requested for volume %s", volumeID)

	if cs.provider == ProviderStatic {
		klog.V(4).Infof("Static provider mode: skipping deletion for volume %s", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	slot := volumeID
	subVolName := "vol-" + volumeID

	// NOTE: DeleteVolumeRequest has no VolumeContext => uses global creds
	restURL := cs.restURL
	username := cs.username
	password := cs.password

	if cs.provider == ProviderNvmeProxy {
		// nvme-proxy PATCH expects bool
		_ = cs.restPatch("/disk/"+slot, map[string]any{"nvmeTcpExport": false}, restURL, username, password)
		_ = cs.restDelete("/disk/"+slot, restURL, username, password)
	} else {
		diskID, _ := cs.getDiskID(slot, restURL, username, password)
		if diskID != "" {
			_ = cs.restPatch("/disk/"+diskID, map[string]any{"nvme-tcp-export": "no"}, restURL, username, password)
			_ = cs.restDelete("/disk/"+diskID, restURL, username, password)
		} else {
			_ = cs.restPatch("/disk/"+slot, map[string]any{"nvme-tcp-export": "no"}, restURL, username, password)
			_ = cs.restDelete("/disk/"+slot, restURL, username, password)
		}
	}

	// Delete subvolume (MikroTik only; nvme-proxy must NOT call these)
	if cs.provider != ProviderNvmeProxy {
		_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, restURL, username, password)
	}

	klog.V(4).Infof("Deletion completed for volume %s (best-effort)", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	cs.initConfig()

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}
	if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "CapacityRange required")
	}

	newBytes := req.GetCapacityRange().GetRequiredBytes()
	if newBytes < minVolumeSize {
		return nil, status.Error(codes.InvalidArgument, "Requested capacity too small")
	}

	klog.V(4).Infof("ControllerExpandVolume requested for volume %s to %d bytes", volumeID, newBytes)

	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "Expansion not supported in static mode")
	}

	giBFloat := math.Ceil(float64(newBytes) / (1024.0 * 1024.0 * 1024.0))
	newGiB := fmt.Sprintf("%dG", int64(giBFloat))
	actualBytes := int64(giBFloat) * 1024 * 1024 * 1024

	// NOTE: uses global creds (no VolumeContext)
	restURL := cs.restURL
	username := cs.username
	password := cs.password

	if cs.provider == ProviderNvmeProxy {
		// nvme-proxy expects PATCH /disk/<slot> with "fileSize"
		if err := cs.restPatch("/disk/"+volumeID, map[string]any{"fileSize": newGiB}, restURL, username, password); err != nil {
			return nil, status.Errorf(codes.Internal, "Expand failed (nvme-proxy): %v", err)
		}
	} else {
		diskID, err := cs.getDiskID(volumeID, restURL, username, password)
		if err != nil || diskID == "" {
			return nil, status.Errorf(codes.Internal, "Failed to get disk .id for expansion: %v", err)
		}
		if err := cs.restPatch("/disk/"+diskID, map[string]any{"file-size": newGiB}, restURL, username, password); err != nil {
			// fallback to SET
			if err2 := cs.restPost("/disk/set", map[string]any{"numbers": diskID, "file-size": newGiB}, restURL, username, password); err2 != nil {
				return nil, status.Errorf(codes.Internal, "Expand failed (PATCH .id: %v, SET .id: %v)", err, err2)
			}
		}
	}

	klog.V(4).Infof("Successfully expanded volume %s to %d bytes", volumeID, actualBytes)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         actualBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *ControllerServer) ListVolumes(_ context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	cs.initConfig()

	if cs.provider == ProviderStatic {
		return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{}}, nil
	}

	disks, err := cs.restGet(cs.diskListPath(), cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "List disks failed: %v", err)
	}

	var volumes []*csi.Volume
	for _, d := range disks {
		slot, _ := d["slot"].(string)
		if slot == "" {
			continue
		}

		// exported?
		exported := false
		if cs.provider == ProviderNvmeProxy {
			// nvme-proxy returns bools for these fields now, but be tolerant.
			switch v := d["nvmeTcpExport"].(type) {
			case bool:
				exported = v
			case string:
				s := strings.ToLower(strings.TrimSpace(v))
				exported = (s == "yes" || s == "true" || s == "1")
			}
		} else {
			if v, ok := d["nvme-tcp-export"].(string); ok {
				exported = (strings.ToLower(strings.TrimSpace(v)) == "yes")
			}
		}

		if !exported {
			continue
		}

		capBytes := parseAnySizeBytes(d)
		volumes = append(volumes, &csi.Volume{
			VolumeId:      slot,
			CapacityBytes: capBytes,
		})
	}

	sort.Slice(volumes, func(i, j int) bool { return volumes[i].VolumeId < volumes[j].VolumeId })

	startIdx := 0
	if token := req.GetStartingToken(); token != "" {
		i, err := strconv.Atoi(token)
		if err != nil || i < 0 || i > len(volumes) {
			return nil, status.Errorf(codes.Aborted, "invalid starting token %q", token)
		}
		startIdx = i
	}

	maxEntries := int(req.GetMaxEntries())
	endIdx := len(volumes)
	if maxEntries > 0 && startIdx+maxEntries < endIdx {
		endIdx = startIdx + maxEntries
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		entries = append(entries, &csi.ListVolumesResponse_Entry{Volume: volumes[i]})
	}

	nextToken := ""
	if endIdx < len(volumes) {
		nextToken = strconv.Itoa(endIdx)
	}

	klog.V(4).Infof("ListVolumes returning %d entries (total exported volumes: %d, nextToken: %s)", len(entries), len(volumes), nextToken)

	return &csi.ListVolumesResponse{Entries: entries, NextToken: nextToken}, nil
}

func (cs *ControllerServer) GetCapacity(_ context.Context, _ *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	cs.initConfig()

	// For nvme-proxy this is best-effort/meaningless unless your API supports it.
	if cs.provider == ProviderStatic || cs.provider == ProviderNvmeProxy {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}

	disks, err := cs.restGet("/disk", cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Capacity query failed: %v", err)
	}

	for _, d := range disks {
		if slot, ok := d["slot"].(string); ok && slot == cs.backendDisk {
			freeStr, _ := d["free"].(string)
			available := parseSizeToBytes(freeStr)
			klog.V(4).Infof("GetCapacity: backend disk %s has %d bytes available", cs.backendDisk, available)
			return &csi.GetCapacityResponse{AvailableCapacity: available}, nil
		}
	}

	klog.Warningf("Backend disk slot %s not found for GetCapacity, reporting 0", cs.backendDisk)
	return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
}

func (cs *ControllerServer) ControllerGetVolume(_ context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	cs.initConfig()

	slot := req.GetVolumeId()
	if slot == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}

	klog.V(4).Infof("ControllerGetVolume requested for volume %s", slot)

	exists, capBytes, err := cs.volumeExists(slot, cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Volume query failed: %v", err)
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "Volume not found")
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{VolumeId: slot, CapacityBytes: capBytes},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{Abnormal: false, Message: "Healthy"},
		},
	}, nil
}

func (cs *ControllerServer) getNvmeProxyNqn(slot, restURL, username, password string) string {
	d, err := cs.restGetOne("/disk/"+slot, restURL, username, password)
	if err != nil {
		klog.Warningf("nvme-proxy GET /disk/%s failed (fallback nqn=slot): %v", slot, err)
		return slot
	}

	coerceString := func(v any) string {
		switch t := v.(type) {
		case string:
			return strings.TrimSpace(t)
		case fmt.Stringer:
			return strings.TrimSpace(t.String())
		default:
			return ""
		}
	}

	// Declare first so it can call itself.
	var tryKey func(obj map[string]any, key string) string

	tryKey = func(obj map[string]any, key string) string {
		v, ok := obj[key]
		if !ok || v == nil {
			return ""
		}

		// direct string
		if s := coerceString(v); s != "" {
			return s
		}

		// array like ["nqn...."] or [{...}]
		if arr, ok := v.([]any); ok && len(arr) > 0 {
			// first element string
			if s := coerceString(arr[0]); s != "" {
				return s
			}
			// first element map, look for nqn-ish fields inside
			if m, ok := arr[0].(map[string]any); ok {
				for _, kk := range []string{"nqn", "subnqn", "subsystemNqn"} {
					if s := tryKey(m, kk); s != "" {
						return s
					}
				}
			}
		}

		// nested map like {"nqn": "..."} or {"subsystem":{"nqn":"..."}}
		if m, ok := v.(map[string]any); ok {
			for _, kk := range []string{"nqn", "subnqn", "subsystemNqn"} {
				if s := tryKey(m, kk); s != "" {
					return s
				}
			}
		}

		return ""
	}

	candidates := []string{
		"nqn",
		"subnqn",
		"subNqn",
		"subsystemNqn",
		"subsystem_nqn",
		"nvmeTcpNqn",
		"nvme_tcp_nqn",
		"exportNqn",
		"export_nqn",
		"targetNqn",
		"target_nqn",
	}

	// 1) direct candidate lookup
	for _, k := range candidates {
		if s := tryKey(d, k); s != "" && s != slot {
			return s
		}
	}

	// 2) scan whole object for any "nqn"-ish key
	for k, v := range d {
		kl := strings.ToLower(strings.TrimSpace(k))
		if strings.Contains(kl, "nqn") {
			if s := coerceString(v); s != "" && s != slot {
				return s
			}
			if m, ok := v.(map[string]any); ok {
				for nk, nv := range m {
					nkl := strings.ToLower(strings.TrimSpace(nk))
					if strings.Contains(nkl, "nqn") {
						if s := coerceString(nv); s != "" && s != slot {
							return s
						}
					}
				}
			}
		}
	}

	// 3) last resort: if it looks like an actual NQN, accept it even if equals slot
	if s := tryKey(d, "nqn"); s != "" && strings.HasPrefix(strings.ToLower(s), "nqn.") {
		return s
	}

	klog.Warningf("nvme-proxy GET /disk/%s returned no usable NQN; falling back to slot", slot)
	return slot
}

// restGetOne should perform a GET that returns a single JSON object (map).
// If your existing restGet is list-oriented (returns []map), implement a separate method
// that decodes to map[string]any for single-object endpoints.
func (cs *ControllerServer) restGetOne(path, restURL, username, password string) (map[string]any, error) {
	body, err := cs.restDo("GET", strings.TrimRight(restURL, "/")+path, nil, username, password)
	if err != nil {
		return nil, err
	}

	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err == nil && len(obj) > 0 {
		return obj, nil
	}

	// If endpoint accidentally returns list, tolerate by picking first element.
	var list []map[string]any
	if err := json.Unmarshal(body, &list); err == nil && len(list) > 0 {
		return list[0], nil
	}

	klog.Errorf("Failed to decode REST GET(one) %s response: %s", path, string(body))
	return nil, fmt.Errorf("failed to decode REST GET(one) %s", path)
}
