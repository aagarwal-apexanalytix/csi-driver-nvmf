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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

const (
	ProviderMikroTik  = "mikrotik"
	ProviderStatic    = "static"
	defaultTargetPort = "4420"
	minVolumeSize     = 1 * 1024 * 1024 // 1 MiB
)

type ControllerServer struct {
	Driver       *driver
	client       *http.Client
	restURL      string
	username     string
	password     string
	backendDisk  string // driver-wide default, overridable per StorageClass
	backendMount string // driver-wide default, overridable per StorageClass
	provider     string // driver-wide ("mikrotik" or "static")
}

func NewControllerServer(d *driver) *ControllerServer {
	cs := &ControllerServer{
		Driver:   d,
		client:   &http.Client{Timeout: 30 * time.Second},
		restURL:  os.Getenv("BACKEND_REST_URL"),
		username: os.Getenv("BACKEND_USERNAME"),
		password: os.Getenv("BACKEND_PASSWORD"),
		provider: strings.ToLower(os.Getenv("CSI_PROVIDER")),
	}

	cs.initConfig()

	klog.V(4).Infof("ControllerServer initialized: provider=%s, backendDisk=%s, backendMount=%s, restURL=%s",
		cs.provider, cs.backendDisk, cs.backendMount, cs.restURL)

	return cs
}

func (cs *ControllerServer) initConfig() {
	if cs.backendDisk == "" {
		cs.backendDisk = "raid1"
	}
	if cs.backendMount == "" {
		cs.backendMount = "btrfs"
	}
	if cs.provider == "" {
		cs.provider = ProviderStatic
	}
}

// Generic REST helper
func (cs *ControllerServer) restDo(method, url string, body []byte, username, password string) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(username, password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := cs.client.Do(req)
	if err != nil {
		klog.Errorf("REST %s %s failed: %v", method, url, err)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		klog.Errorf("REST %s %s error %d: %s", method, url, resp.StatusCode, string(respBody))
		return nil, fmt.Errorf("REST error %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// Helper wrappers
func (cs *ControllerServer) restGet(path, restURL, username, password string) ([]map[string]interface{}, error) {
	body, err := cs.restDo("GET", restURL+path, nil, username, password)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		klog.Errorf("Failed to decode REST GET %s: %v", path, err)
		return nil, err
	}

	klog.V(5).Infof("REST GET %s returned %d items", path, len(result))
	return result, nil
}

func (cs *ControllerServer) restPost(path string, data map[string]string, restURL, username, password string) error {
	jsonBody, _ := json.Marshal(data)
	_, err := cs.restDo("POST", restURL+path, jsonBody, username, password)
	return err
}

func (cs *ControllerServer) restPatch(path string, data map[string]string, restURL, username, password string) error {
	jsonBody, _ := json.Marshal(data)
	_, err := cs.restDo("PATCH", restURL+path, jsonBody, username, password)
	return err
}

func (cs *ControllerServer) restDelete(path, restURL, username, password string) error {
	_, err := cs.restDo("DELETE", restURL+path, nil, username, password)
	return err
}

// Get .id for a disk by slot
func (cs *ControllerServer) getDiskID(slot, restURL, username, password string) (string, error) {
	disks, err := cs.restGet("/disk", restURL, username, password)
	if err != nil {
		return "", err
	}

	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == slot {
			id, ok := d[".id"].(string)
			if ok && id != "" {
				return id, nil
			}
		}
	}
	return "", fmt.Errorf("disk with slot %s not found or missing .id", slot)
}

// Parse size string to bytes
func parseSizeToBytes(s string) int64 {
	if s == "" {
		return 0
	}
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.TrimSuffix(s, "GIB")
	s = strings.TrimSuffix(s, "GI")
	s = strings.TrimSuffix(s, "G")

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		klog.Warningf("Failed to parse size string '%s': %v", s, err)
		return 0
	}
	return int64(f * 1024 * 1024 * 1024)
}

// Check if volume/slot exists
func (cs *ControllerServer) volumeExists(volumeID, restURL, username, password string) (bool, int64, error) {
	klog.V(5).Infof("Checking existence of volume/slot %s", volumeID)

	disks, err := cs.restGet("/disk", restURL, username, password)
	if err != nil {
		return false, 0, err
	}

	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == volumeID {
			sizeStr, _ := d["file-size"].(string)
			if sizeStr == "" {
				sizeStr, _ = d["size"].(string)
			}
			sizeBytes := parseSizeToBytes(sizeStr)
			klog.V(5).Infof("Volume/slot %s exists with size %d bytes", volumeID, sizeBytes)
			return true, sizeBytes, nil
		}
	}

	klog.V(5).Infof("Volume/slot %s does not exist", volumeID)
	return false, 0, nil
}

func (cs *ControllerServer) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	cs.initConfig()

	klog.V(4).Infof("CreateVolume requested: name=%s, capacityRange=%v, parameters=%v, contentSource=%v",
		req.GetName(), req.CapacityRange, req.Parameters, req.VolumeContentSource)

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name required")
	}

	capBytes := req.CapacityRange.GetRequiredBytes()
	if capBytes == 0 || capBytes < minVolumeSize {
		capBytes = minVolumeSize
	}

	params := req.Parameters

	targetAddr, ok := params["targetAddr"]
	if !ok || targetAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "targetAddr is required in StorageClass parameters")
	}

	targetPort := params["targetPort"]
	if targetPort == "" {
		targetPort = defaultTargetPort
	}

	backendDisk := params["backendDisk"]
	if backendDisk == "" {
		backendDisk = cs.backendDisk
	}
	backendMount := params["backendMount"]
	if backendMount == "" {
		backendMount = cs.backendMount
	}

	volumeID := strings.ReplaceAll(strings.ToLower(req.Name), "-", "")
	slot := volumeID
	subVolName := "vol-" + volumeID
	imgPath := backendMount + "/" + subVolName + "/volume.img"
	sizeGiB := fmt.Sprintf("%dG", capBytes/(1024*1024*1024))

	var fromSnapshot string
	if source := req.GetVolumeContentSource(); source != nil {
		if snap := source.GetSnapshot(); snap != nil {
			fromSnapshot = snap.SnapshotId
		}
		if vol := source.GetVolume(); vol != nil {
			return nil, status.Error(codes.InvalidArgument, "Volume cloning not supported")
		}
	}

	if cs.provider == ProviderStatic {
		exists, currentSize, _ := cs.volumeExists(volumeID, cs.restURL, cs.username, cs.password)
		if !exists {
			return nil, status.Error(codes.NotFound, "Volume not pre-created in static mode")
		}
		if currentSize < capBytes {
			return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested (static mode)")
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capBytes,
				VolumeContext: map[string]string{
					"targetTrAddr": targetAddr,
					"targetTrPort": targetPort,
					"targetTrType": "tcp",
					"nqn":          slot,
					"deviceID":     slot,
				},
			},
		}, nil
	}

	localRestURL := params["restURL"]
	if localRestURL == "" {
		localRestURL = cs.restURL
	}
	if localRestURL == "" {
		return nil, status.Error(codes.InvalidArgument, "restURL required")
	}

	localUsername := params["username"]
	if localUsername == "" {
		localUsername = cs.username
	}
	if localUsername == "" {
		return nil, status.Error(codes.InvalidArgument, "username required")
	}

	localPassword := params["password"]
	if localPassword == "" {
		localPassword = cs.password
	}
	if localPassword == "" {
		return nil, status.Error(codes.InvalidArgument, "password required")
	}

	klog.V(4).Infof("Using MikroTik REST API: url=%s, username=%s, password_length=%d", localRestURL, localUsername, len(localPassword))

	// Idempotency check
	exists, currentSize, err := cs.volumeExists(volumeID, localRestURL, localUsername, localPassword)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to check existing volume: %v", err)
	}
	if exists {
		if currentSize >= capBytes {
			klog.V(4).Infof("Volume %s already exists with sufficient size (%d >= %d bytes)", volumeID, currentSize, capBytes)
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      volumeID,
					CapacityBytes: currentSize,
					VolumeContext: map[string]string{
						"targetTrAddr": targetAddr,
						"targetTrPort": targetPort,
						"targetTrType": "tcp",
						"nqn":          slot,
						"deviceID":     slot,
					},
				},
			}, nil
		}
		return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested")
	}

	klog.V(4).Infof("Provisioning new volume %s (slot=%s, size=%s, fromSnapshot=%s)", req.Name, slot, sizeGiB, fromSnapshot)

	// Create BTRFS subvolume
	subvolData := map[string]string{
		"fs":   backendDisk,
		"name": subVolName,
	}
	if fromSnapshot != "" {
		subvolData["parent"] = fromSnapshot
		subvolData["read-only"] = "yes"
	}
	err = cs.restPost("/disk/btrfs/subvolume/add", subvolData, localRestURL, localUsername, localPassword)
	if err != nil {
		if strings.Contains(err.Error(), "exists") || strings.Contains(err.Error(), "File exists") {
			klog.V(4).Infof("Subvolume %s already exists (idempotent)", subVolName)
		} else {
			return nil, status.Errorf(codes.Internal, "Subvolume create failed: %v", err)
		}
	}

	// Create file-backed disk
	diskData := map[string]string{
		"type":      "file",
		"file-path": imgPath,
		"file-size": sizeGiB,
		"slot":      slot,
	}
	if err := cs.restPost("/disk/add", diskData, localRestURL, localUsername, localPassword); err != nil {
		_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
		return nil, status.Errorf(codes.Internal, "Disk create failed: %v", err)
	}

	// Get .id for reliable addressing
	diskID, err := cs.getDiskID(slot, localRestURL, localUsername, localPassword)
	if err != nil {
		// Best-effort cleanup using slot fallback
		_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
		_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
		return nil, status.Errorf(codes.Internal, "Failed to retrieve disk .id for export: %v", err)
	}

	// Enable NVMe-TCP export using .id
	exportData := map[string]string{
		"nvme-tcp-export": "yes",
		"nvme-tcp-port":   targetPort,
	}
	err = cs.restPatch("/disk/"+diskID, exportData, localRestURL, localUsername, localPassword)
	if err != nil {
		klog.Warningf("PATCH export by .id failed, trying SET by .id: %v", err)
		setData := map[string]string{
			"numbers":         diskID,
			"nvme-tcp-export": "yes",
			"nvme-tcp-port":   targetPort,
		}
		if err2 := cs.restPost("/disk/set", setData, localRestURL, localUsername, localPassword); err2 != nil {
			// Cleanup
			_ = cs.restDelete("/disk/"+diskID, localRestURL, localUsername, localPassword)
			_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			return nil, status.Errorf(codes.Internal, "Export enable failed (PATCH .id: %v, SET .id: %v)", err, err2)
		}
	}

	klog.V(4).Infof("Successfully created volume %s (size %d bytes)", volumeID, capBytes)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capBytes,
			VolumeContext: map[string]string{
				"targetTrAddr": targetAddr,
				"targetTrPort": targetPort,
				"targetTrType": "tcp",
				"nqn":          slot,
				"deviceID":     slot,
			},
		},
	}, nil
}

func (cs *ControllerServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	cs.initConfig()

	volumeID := req.VolumeId
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

	// Best-effort disable export
	diskID, _ := cs.getDiskID(slot, cs.restURL, cs.username, cs.password)
	if diskID != "" {
		_ = cs.restPatch("/disk/"+diskID, map[string]string{"nvme-tcp-export": "no"}, cs.restURL, cs.username, cs.password)
		_ = cs.restDelete("/disk/"+diskID, cs.restURL, cs.username, cs.password)
	} else {
		// Fallback to slot
		_ = cs.restPatch("/disk/"+slot, map[string]string{"nvme-tcp-export": "no"}, cs.restURL, cs.username, cs.password)
		_ = cs.restDelete("/disk/"+slot, cs.restURL, cs.username, cs.password)
	}

	// Delete subvolume
	_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, cs.restURL, cs.username, cs.password)

	klog.V(4).Infof("Deletion completed for volume %s (best-effort)", volumeID)
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	cs.initConfig()

	slot := req.VolumeId
	if slot == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}

	newBytes := req.CapacityRange.GetRequiredBytes()
	if newBytes < minVolumeSize {
		return nil, status.Error(codes.InvalidArgument, "Requested capacity too small")
	}

	klog.V(4).Infof("ControllerExpandVolume requested for volume %s to %d bytes", slot, newBytes)

	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "Expansion not supported in static mode")
	}

	newGiB := fmt.Sprintf("%dG", newBytes/(1024*1024*1024))

	// Get .id
	diskID, err := cs.getDiskID(slot, cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get disk .id for expansion: %v", err)
	}

	// Try PATCH by .id
	err = cs.restPatch("/disk/"+diskID, map[string]string{"file-size": newGiB}, cs.restURL, cs.username, cs.password)
	if err != nil {
		// Fallback SET by .id
		if err2 := cs.restPost("/disk/set", map[string]string{
			"numbers":   diskID,
			"file-size": newGiB,
		}, cs.restURL, cs.username, cs.password); err2 != nil {
			return nil, status.Errorf(codes.Internal, "Expand failed (PATCH .id: %v, SET .id: %v)", err, err2)
		}
	}

	klog.V(4).Infof("Successfully expanded volume %s to %d bytes", slot, newBytes)
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *ControllerServer) ListVolumes(_ context.Context, _ *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	cs.initConfig()

	if cs.provider == ProviderStatic {
		return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{}}, nil
	}

	disks, err := cs.restGet("/disk", cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "List disks failed: %v", err)
	}

	var entries []*csi.ListVolumesResponse_Entry
	for _, d := range disks {
		slot, _ := d["slot"].(string)
		if slot == "" || !strings.HasPrefix(slot, "pvc") {
			continue
		}

		export, _ := d["nvme-tcp-export"].(string)
		if export != "yes" {
			continue
		}

		sizeStr, _ := d["file-size"].(string)
		if sizeStr == "" {
			sizeStr, _ = d["size"].(string)
		}
		capBytes := parseSizeToBytes(sizeStr)

		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      slot,
				CapacityBytes: capBytes,
			},
		})
	}

	klog.V(4).Infof("ListVolumes returning %d volumes", len(entries))
	return &csi.ListVolumesResponse{Entries: entries}, nil
}

func (cs *ControllerServer) GetCapacity(_ context.Context, _ *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	cs.initConfig()

	if cs.provider == ProviderStatic {
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

	slot := req.VolumeId
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

func (cs *ControllerServer) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "VolumeId required")
	}

	klog.V(5).Infof("ValidateVolumeCapabilities requested for volume %s with %d capabilities", req.VolumeId, len(req.VolumeCapabilities))

	supported := true
	for _, cap := range req.VolumeCapabilities {
		if cap.GetBlock() == nil && cap.GetMount() == nil {
			supported = false
			continue
		}

		mode := cap.GetAccessMode().GetMode()

		if cap.GetBlock() != nil {
			// Raw block: allow RWO, RWX (for KubeVirt migration), RWOP
			if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				supported = false
			}
		} else {
			// Filesystem mount: allow RWO and RWOP only (no multi-node sharing)
			if mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER &&
				mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				supported = false
			}
		}
	}

	if supported {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
				VolumeCapabilities: req.VolumeCapabilities,
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

func (cs *ControllerServer) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	cs.initConfig()

	sourceVol := req.SourceVolumeId
	snapName := req.Name
	if sourceVol == "" || snapName == "" {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId and Name required")
	}

	klog.V(4).Infof("CreateSnapshot requested: name=%s, sourceVolumeId=%s, parameters=%v", snapName, sourceVol, req.Parameters)

	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "Snapshots not supported in static mode")
	}

	params := req.Parameters

	localRestURL := params["restURL"]
	if localRestURL == "" {
		localRestURL = cs.restURL
	}
	if localRestURL == "" {
		return nil, status.Error(codes.InvalidArgument, "restURL required")
	}

	localUsername := params["username"]
	if localUsername == "" {
		localUsername = cs.username
	}
	if localUsername == "" {
		return nil, status.Error(codes.InvalidArgument, "username required")
	}

	localPassword := params["password"]
	if localPassword == "" {
		localPassword = cs.password
	}
	if localPassword == "" {
		return nil, status.Error(codes.InvalidArgument, "password required")
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

	klog.V(4).Infof("Successfully created snapshot %s from volume %s", snapSubVol, sourceVol)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapSubVol,
			SourceVolumeId: sourceVol,
			CreationTime:   timestamppb.New(time.Now()),
			ReadyToUse:     true,
			SizeBytes:      0,
		},
	}, nil
}

func (cs *ControllerServer) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	cs.initConfig()

	snapSubVol := req.SnapshotId
	if snapSubVol == "" {
		return nil, status.Error(codes.InvalidArgument, "SnapshotId required")
	}

	klog.V(4).Infof("DeleteSnapshot requested for snapshot %s", snapSubVol)

	if cs.provider == ProviderStatic {
		return &csi.DeleteSnapshotResponse{}, nil
	}

	_ = cs.restDelete("/disk/btrfs/subvolume/"+snapSubVol, cs.restURL, cs.username, cs.password)

	klog.V(4).Infof("Snapshot %s deleted (best-effort)", snapSubVol)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(_ context.Context, _ *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	cs.initConfig()

	klog.V(4).Infof("ListSnapshots requested (placeholder implementation)")

	return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{}}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(5).Infof("ControllerGetCapabilities requested")

	caps := []*csi.ControllerServiceCapability{
		{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME}}},
		{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME}}},
		{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES}}},
		{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY}}},
		{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_GET_VOLUME}}},
	}

	if cs.provider != ProviderStatic {
		caps = append(caps,
			&csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT}}},
			&csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS}}},
		)
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}
