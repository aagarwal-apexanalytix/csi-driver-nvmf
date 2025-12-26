// Package nvmf
package nvmf

import (
	"context"
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
	klog.V(4).Infof("CreateVolume requested: name=%s, capacityRange=%v, parameters=%v, contentSource=%v",
		req.GetName(), req.CapacityRange, req.Parameters, req.VolumeContentSource)

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name required")
	}

	contentSource := req.GetVolumeContentSource()
	params := req.Parameters

	// fstype
	fstype := params["csi.storage.k8s.io/fstype"]
	if fstype == "" {
		fstype = params["fstype"]
	}
	if fstype == "" {
		fstype = "ext4"
	}
	if fstype != "ext4" && fstype != "xfs" {
		klog.Warningf("Requested fstype %q is not explicitly supported by online resize logic (supported: ext4, xfs)", fstype)
	}

	// NVMe target
	targetAddr := strings.TrimSpace(params["targetAddr"])
	if targetAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "targetAddr is required in StorageClass parameters")
	}

	targetPortStr := strings.TrimSpace(params["targetPort"])
	if targetPortStr == "" {
		targetPortStr = defaultTargetPort
	}
	targetPortInt, err := strconv.Atoi(strings.TrimSpace(targetPortStr))
	if err != nil || targetPortInt <= 0 || targetPortInt > 65535 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid targetPort %q (must be 1-65535)", targetPortStr)
	}

	// backend config
	backendDisk := strings.TrimSpace(params["backendDisk"])
	if backendDisk == "" {
		backendDisk = cs.backendDisk
	}
	backendMount := strings.TrimSpace(params["backendMount"])
	if backendMount == "" {
		backendMount = cs.backendMount
	}

	// CSI metadata
	pvcNamespace := params["csi.storage.k8s.io/pvc/namespace"]
	pvcHumanName := params["csi.storage.k8s.io/pvc/name"]
	pvName := params["csi.storage.k8s.io/pv/name"]
	if pvcNamespace == "" {
		klog.Warningf("PVC namespace param missing - ensure csi-provisioner has --extra-create-metadata=true")
		pvcNamespace = "unknown"
	}
	if pvcHumanName == "" {
		klog.Warningf("PVC name param missing - falling back to req.Name")
		pvcHumanName = req.Name
	}
	if pvName == "" {
		klog.Warningf("PV name param missing - falling back to req.Name")
		pvName = req.Name
	}
	clusterName := params["clusterName"]
	if clusterName == "" {
		clusterName = "unknown"
	}

	// per-SC endpoint/creds
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

	// nvme-proxy often runs with no auth; static doesn't require auth
	if cs.provider != ProviderNvmeProxy && cs.provider != ProviderStatic {
		if strings.TrimSpace(localUsername) == "" {
			return nil, status.Error(codes.InvalidArgument, "username required (no global configured)")
		}
		if strings.TrimSpace(localPassword) == "" {
			return nil, status.Error(codes.InvalidArgument, "password required (no global configured)")
		}
	}

	// cloning / restore
	var parentSubvol string
	var sourceCapacity int64
	var cloneInfo string
	if contentSource != nil {
		if cs.provider == ProviderStatic {
			return nil, status.Error(codes.Unimplemented, "Volume cloning/from-snapshot not supported in static mode")
		}
		var err error
		parentSubvol, sourceCapacity, cloneInfo, err = cs.handleContentSource(contentSource, localRestURL, localUsername, localPassword, &fstype)
		if err != nil {
			return nil, err
		}
	}

	comment := fmt.Sprintf("k8s-pvc:%s:%s/%s (pv:%s) fstype:%s%s", clusterName, pvcNamespace, pvcHumanName, pvName, fstype, cloneInfo)

	// IDs
	volumeID := strings.ReplaceAll(strings.ToLower(pvName), "-", "")
	slot := volumeID
	subVolName := "vol-" + volumeID

	// MikroTik legacy file path under mount
	imgPath := backendMount + "/" + subVolName + "/volume.img"

	// effective capacity
	requiredBytes := int64(0)
	if req.CapacityRange != nil {
		requiredBytes = req.CapacityRange.GetRequiredBytes()
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

	// STATIC
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
				if diskID, getErr := cs.getDiskID(slot, localRestURL, localUsername, localPassword); getErr == nil {
					_ = cs.restPost("/disk/set", map[string]any{"numbers": diskID, "comment": comment}, localRestURL, localUsername, localPassword)
				}
			}

			volCtx := map[string]string{
				"targetTrAddr":              targetAddr,
				"targetTrPort":              targetPortStr,
				"targetTrType":              "tcp",
				"nqn":                       slot,
				"deviceID":                  slot,
				"csi.storage.k8s.io/fstype": fstype,
			}
			if cs.provider == ProviderNvmeProxy {
				if v := nvmeProxyCompressFromParams(params); v != "" {
					volCtx["compression"] = v
				}
			} else {
				volCtx["compression"] = getEffectiveCompress(params)
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

	klog.V(4).Infof("Provisioning new volume %s (slot=%s, size=%s)%s", req.Name, slot, sizeGiB, cloneInfo)

	// nvme-proxy can be BTRFS OR ZFS; if backendMount == "zfs" skip BTRFS subvol calls entirely
	skipSubvol := (cs.provider == ProviderNvmeProxy && strings.EqualFold(strings.TrimSpace(backendMount), "zfs"))

	// Create BTRFS subvolume (empty or snapshot) if applicable
	if !skipSubvol {
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
	}

	var compressForContext string

	// Provider-specific disk create + export
	if cs.provider == ProviderNvmeProxy {
		// nvme-proxy: POST /disk/add
		//
		// IMPORTANT: server-side types (per your errors):
		// - nvmeTcpPort: int
		// - nvmeTcpExport: string ("yes"/"no" or "true"/"false" depending on server; "yes" is safest)
		diskData := map[string]any{
			"slot":          slot,
			"fileSize":      sizeGiB,
			"comment":       comment,
			"nvmeTcpExport": "yes",
			"nvmeTcpPort":   targetPortInt,
		}
		if c := nvmeProxyCompressFromParams(params); c != "" {
			diskData["compress"] = c
			compressForContext = c
			comment += fmt.Sprintf(" compress:%s", c)
		}
		if err := cs.restPost("/disk/add", diskData, localRestURL, localUsername, localPassword); err != nil {
			_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
			if !skipSubvol {
				_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			}
			return nil, status.Errorf(codes.Internal, "nvme-proxy disk create failed: %v", err)
		}

		// best-effort ensure export enabled (types matter)
		_ = cs.restPatch("/disk/"+slot, map[string]any{
			"nvmeTcpExport": "yes",
			"nvmeTcpPort":   targetPortInt,
		}, localRestURL, localUsername, localPassword)

	} else {
		// MikroTik: create file-backed disk; then enable export via PATCH/SET; then compression via /disk/set
		diskData := map[string]any{
			"type":      "file",
			"file-path": imgPath,
			"file-size": sizeGiB,
			"slot":      slot,
		}
		if err := cs.restPost("/disk/add", diskData, localRestURL, localUsername, localPassword); err != nil {
			if !skipSubvol {
				_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			}
			return nil, status.Errorf(codes.Internal, "Disk create failed: %v", err)
		}

		diskID, err := cs.getDiskID(slot, localRestURL, localUsername, localPassword)
		if err != nil {
			_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
			if !skipSubvol {
				_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			}
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
				if !skipSubvol {
					_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
				}
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
				klog.Warningf("Failed to set compress=%s on disk %s (requested: %s): %v (continuing anyway)",
					compressValue, diskID, params["compression"], err)
			} else {
				klog.Infof("Successfully set compress=%s on disk %s for volume %s (requested: %s)",
					compressValue, diskID, volumeID, params["compression"])
			}
			comment += fmt.Sprintf(" compress:%s", compressValue)
		}

		// Comment
		if err := cs.restPost("/disk/set", map[string]any{
			"numbers": diskID,
			"comment": comment,
		}, localRestURL, localUsername, localPassword); err != nil {
			klog.Warningf("Failed to set comment (non-critical): %v", err)
		}
	}

	klog.V(4).Infof("Successfully created volume %s (actual size %d bytes)%s", volumeID, actualBytes, cloneInfo)

	volCtx := map[string]string{
		"targetTrAddr":              targetAddr,
		"targetTrPort":              targetPortStr,
		"targetTrType":              "tcp",
		"nqn":                       slot,
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

	// NOTE: delete currently uses global creds because DeleteVolumeRequest has no VolumeContext.
	restURL := cs.restURL
	username := cs.username
	password := cs.password

	// Disable export + delete disk
	if cs.provider == ProviderNvmeProxy {
		// IMPORTANT: server-side expects STRING for nvmeTcpExport (per your error)
		_ = cs.restPatch("/disk/"+slot, map[string]any{"nvmeTcpExport": "no"}, restURL, username, password)
		_ = cs.restDelete("/disk/"+slot, restURL, username, password)
	} else {
		diskID, _ := cs.getDiskID(slot, restURL, username, password)
		if diskID != "" {
			_ = cs.restPatch("/disk/"+diskID, map[string]any{"nvme-tcp-export": "no"}, restURL, username, password)
			_ = cs.restDelete("/disk/"+diskID, restURL, username, password)
		} else {
			// best-effort fallback
			_ = cs.restPatch("/disk/"+slot, map[string]any{"nvme-tcp-export": "no"}, restURL, username, password)
			_ = cs.restDelete("/disk/"+slot, restURL, username, password)
		}
	}

	// Delete subvolume (no-op-ish for nvme-proxy zfs backend; the endpoint might not exist)
	_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, restURL, username, password)

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

	newBytes := req.CapacityRange.GetRequiredBytes()
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

	// NOTE: expand uses global creds (no VolumeContext in request)
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
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get disk .id for expansion: %v", err)
		}
		if err := cs.restPatch("/disk/"+diskID, map[string]any{"file-size": newGiB}, restURL, username, password); err != nil {
			// fallback to SET
			if err2 := cs.restPost("/disk/set", map[string]any{
				"numbers":   diskID,
				"file-size": newGiB,
			}, restURL, username, password); err2 != nil {
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
			// nvme-proxy might return string/bool
			switch v := d["nvmeTcpExport"].(type) {
			case bool:
				exported = v
			case string:
				exported = (strings.ToLower(v) == "yes" || strings.ToLower(v) == "true")
			}
		} else {
			if v, ok := d["nvme-tcp-export"].(string); ok {
				exported = (v == "yes")
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

	// For nvme-proxy this may be meaningless unless API supports it; keep as best-effort for MikroTik.
	if cs.provider == ProviderStatic {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}
	if cs.provider == ProviderNvmeProxy {
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
		Volume: &csi.Volume{
			VolumeId:      slot,
			CapacityBytes: capBytes,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: false,
				Message:  "Healthy",
			},
		},
	}, nil
}
