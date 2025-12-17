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

	klog.V(4).Infof("ControllerServer initialized: provider=%s, backendDisk=%s, backendMount=%s, restURL=%s (targetAddr/targetPort per StorageClass; credentials/restURL overridable per StorageClass with ENV fallback)",
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

// Helper: Execute command via REST /execute endpoint and read response for errors (driver-wide config)
func (cs *ControllerServer) executeCommand(cmd string) error {
	if cs.restURL == "" {
		return fmt.Errorf("backend REST URL not configured")
	}
	if cs.provider == ProviderStatic {
		return fmt.Errorf("dynamic operations not supported in static mode")
	}

	klog.V(5).Infof("Executing backend command: %s", cmd)

	body := map[string]string{"command": cmd}
	jsonBody, _ := json.Marshal(body)

	req, err := http.NewRequest("POST", cs.restURL+"/execute", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.SetBasicAuth(cs.username, cs.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := cs.client.Do(req)
	if err != nil {
		klog.Errorf("HTTP request failed for command '%s': %v", cmd, err)
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		klog.Errorf("Backend returned error status %d for command '%s': body=%s", resp.StatusCode, cmd, string(respBody))
		return fmt.Errorf("execute error: status %d, body: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if json.Unmarshal(respBody, &result) == nil {
		if errMsg, ok := result["error"].(string); ok && errMsg != "" {
			klog.Errorf("Backend command failed '%s': %s", cmd, errMsg)
			return fmt.Errorf("command failed: %s", errMsg)
		}
	}

	klog.V(5).Infof("Backend command succeeded: %s", cmd)
	return nil
}

// Helper: GET for listing (driver-wide config)
func (cs *ControllerServer) restGet(path string) ([]map[string]interface{}, error) {
	if cs.restURL == "" {
		return nil, fmt.Errorf("backend REST URL not configured")
	}

	klog.V(5).Infof("Performing REST GET: %s%s", cs.restURL, path)

	req, err := http.NewRequest("GET", cs.restURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(cs.username, cs.password)

	resp, err := cs.client.Do(req)
	if err != nil {
		klog.Errorf("REST GET request failed for %s: %v", path, err)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		klog.Errorf("REST GET error for %s: status %d, body=%s", path, resp.StatusCode, string(respBody))
		return nil, fmt.Errorf("GET error: %d, body: %s", resp.StatusCode, string(respBody))
	}

	var result []map[string]interface{}
	if err := json.NewDecoder(bytes.NewReader(respBody)).Decode(&result); err != nil {
		klog.Errorf("Failed to decode REST GET response for %s: %v", path, err)
		return nil, err
	}

	klog.V(5).Infof("REST GET succeeded for %s, returned %d items", path, len(result))
	return result, nil
}

// Helper: Parse size string to bytes
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

// Check if volume/slot exists and return size
func (cs *ControllerServer) volumeExists(volumeID string) (bool, int64, error) {
	klog.V(5).Infof("Checking existence of volume/slot %s", volumeID)

	if cs.provider == ProviderStatic {
		klog.V(5).Infof("Static provider mode: assuming volume %s exists (size unknown)", volumeID)
		return true, 0, nil
	}

	disks, err := cs.restGet("/disk")
	if err != nil {
		return false, 0, err
	}

	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == volumeID {
			sizeStr, _ := d["file-size"].(string)
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

	// Required: targetAddr
	targetAddr, ok := params["targetAddr"]
	if !ok || targetAddr == "" {
		klog.Errorf("targetAddr is required in StorageClass parameters for volume %s", req.Name)
		return nil, status.Error(codes.InvalidArgument, "targetAddr is required in StorageClass parameters")
	}

	// Optional: targetPort (default 4420)
	targetPort := params["targetPort"]
	if targetPort == "" {
		targetPort = defaultTargetPort
	}

	// Optional overrides for backendDisk / backendMount (fallback to driver-wide defaults)
	backendDisk := params["backendDisk"]
	if backendDisk == "" {
		backendDisk = cs.backendDisk
	}
	backendMount := params["backendMount"]
	if backendMount == "" {
		backendMount = cs.backendMount
	}

	volumeID := "pvc-" + strings.ReplaceAll(strings.ToLower(req.Name), "-", "")
	slot := volumeID

	var exists bool
	var currentSize int64

	if cs.provider == ProviderStatic {
		exists = true
		currentSize = 0
	} else {
		// Dynamic mode: credential / REST URL override support
		localRestURL := params["restURL"]
		if localRestURL == "" {
			localRestURL = cs.restURL
		}
		if localRestURL == "" {
			return nil, status.Error(codes.InvalidArgument, "restURL required (in StorageClass parameters or BACKEND_REST_URL env)")
		}

		localUsername := params["username"]
		if localUsername == "" {
			localUsername = cs.username
		}
		if localUsername == "" {
			return nil, status.Error(codes.InvalidArgument, "username required (in StorageClass parameters or BACKEND_USERNAME env)")
		}

		localPassword := params["password"]
		if localPassword == "" {
			localPassword = cs.password
		}
		if localPassword == "" {
			return nil, status.Error(codes.InvalidArgument, "password required (in StorageClass parameters or BACKEND_PASSWORD env)")
		}

		// Debug logging for credentials (password length only)
		klog.V(4).Infof("Using MikroTik REST API: url=%s, username=%s, password_length=%d", localRestURL, localUsername, len(localPassword))

		// Local restGet with improved error logging
		localRestGet := func(path string) ([]map[string]interface{}, error) {
			klog.V(5).Infof("Performing local REST GET: %s%s", localRestURL, path)
			req, err := http.NewRequest("GET", localRestURL+path, nil)
			if err != nil {
				return nil, err
			}
			req.SetBasicAuth(localUsername, localPassword)

			resp, err := cs.client.Do(req)
			if err != nil {
				klog.Errorf("Local REST GET failed for %s: %v", path, err)
				return nil, err
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				klog.Errorf("Local REST GET error for %s: status %d, body=%s", path, resp.StatusCode, string(respBody))
				return nil, fmt.Errorf("local GET error: %d, body: %s", resp.StatusCode, string(respBody))
			}

			var result []map[string]interface{}
			if err := json.NewDecoder(bytes.NewReader(respBody)).Decode(&result); err != nil {
				klog.Errorf("Failed to decode local REST GET response for %s: %v", path, err)
				return nil, err
			}
			return result, nil
		}

		disks, err := localRestGet("/disk")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to list disks for idempotency check: %v", err)
		}

		for _, d := range disks {
			if s, ok := d["slot"].(string); ok && s == volumeID {
				sizeStr, _ := d["file-size"].(string)
				currentSize = parseSizeToBytes(sizeStr)
				exists = true
				break
			}
		}

		// Local executeCommand with improved error logging
		localExecute := func(cmd string) error {
			klog.V(5).Infof("Executing local backend command: %s", cmd)
			body := map[string]string{"command": cmd}
			jsonBody, _ := json.Marshal(body)

			req, err := http.NewRequest("POST", localRestURL+"/execute", bytes.NewReader(jsonBody))
			if err != nil {
				return err
			}
			req.SetBasicAuth(localUsername, localPassword)
			req.Header.Set("Content-Type", "application/json")

			resp, err := cs.client.Do(req)
			if err != nil {
				klog.Errorf("Local HTTP request failed for command '%s': %v", cmd, err)
				return fmt.Errorf("HTTP request failed: %w", err)
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 300 {
				klog.Errorf("Local backend error status %d for command '%s': body=%s", resp.StatusCode, cmd, string(respBody))
				return fmt.Errorf("execute error: status %d, body: %s", resp.StatusCode, string(respBody))
			}

			var result map[string]interface{}
			if json.Unmarshal(respBody, &result) == nil {
				if errMsg, ok := result["error"].(string); ok && errMsg != "" {
					klog.Errorf("Local backend command failed '%s': %s", cmd, errMsg)
					return fmt.Errorf("command failed: %s", errMsg)
				}
			}

			klog.V(5).Infof("Local backend command succeeded: %s", cmd)
			return nil
		}

		// Idempotency check
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
						},
					},
				}, nil
			}
			return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested")
		}

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

		klog.V(4).Infof("Provisioning new volume %s (slot=%s, size=%s, fromSnapshot=%s)", req.Name, slot, sizeGiB, fromSnapshot)

		// Create subvolume
		var subVolCmd string
		if fromSnapshot != "" {
			subVolCmd = fmt.Sprintf("/disk/btrfs/subvolume/add parent=%s fs=%s name=%s", fromSnapshot, backendDisk, subVolName)
		} else {
			subVolCmd = fmt.Sprintf("/disk/btrfs/subvolume/add fs=%s name=%s", backendDisk, subVolName)
		}
		if err := localExecute(subVolCmd); err != nil {
			return nil, status.Errorf(codes.Internal, "Subvolume create failed: %v", err)
		}

		// Create .img file
		if err := localExecute(fmt.Sprintf("/disk add type=file file-path=%s file-size=%s slot=%s", imgPath, sizeGiB, slot)); err != nil {
			_ = localExecute(fmt.Sprintf("/disk/btrfs/subvolume/remove [find name=%s]", subVolName))
			return nil, status.Errorf(codes.Internal, ".img create failed: %v", err)
		}

		// Enable export
		if err := localExecute(fmt.Sprintf("/disk set %s nvme-tcp-export=yes nvme-tcp-port=%s", slot, targetPort)); err != nil {
			_ = localExecute(fmt.Sprintf("/disk remove %s", slot))
			_ = localExecute(fmt.Sprintf("/disk/btrfs/subvolume/remove [find name=%s]", subVolName))
			return nil, status.Errorf(codes.Internal, "Export failed: %v", err)
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
				},
			},
		}, nil
	}

	// Static provider path
	if exists {
		if currentSize >= capBytes {
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      volumeID,
					CapacityBytes: capBytes,
					VolumeContext: map[string]string{
						"targetTrAddr": targetAddr,
						"targetTrPort": targetPort,
						"targetTrType": "tcp",
						"nqn":          slot,
					},
				},
			}, nil
		}
		return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested (static mode)")
	}

	klog.V(4).Infof("Static provider mode: volume %s not pre-created", volumeID)
	return nil, status.Error(codes.NotFound, "Volume not pre-created in static mode")
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

	// Best-effort cleanup
	klog.V(4).Infof("Disabling NVMe-TCP export for slot %s", slot)
	_ = cs.executeCommand(fmt.Sprintf("/disk set %s nvme-tcp-export=no", slot))

	klog.V(4).Infof("Removing disk slot %s", slot)
	_ = cs.executeCommand(fmt.Sprintf("/disk remove %s", slot))

	klog.V(4).Infof("Removing btrfs subvolume %s", subVolName)
	_ = cs.executeCommand(fmt.Sprintf("/disk/btrfs/subvolume/remove [find name=%s]", subVolName))

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
	if err := cs.executeCommand(fmt.Sprintf("/disk set %s file-size=%s", slot, newGiB)); err != nil {
		return nil, status.Errorf(codes.Internal, "Expand failed: %v", err)
	}

	klog.V(4).Infof("Successfully expanded volume %s to %d bytes", slot, newBytes)
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *ControllerServer) ListVolumes(_ context.Context, _ *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	cs.initConfig()

	klog.V(4).Infof("ListVolumes requested")

	if cs.provider == ProviderStatic {
		return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{}}, nil
	}

	disks, err := cs.restGet("/disk")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "List disks failed: %v", err)
	}

	var entries []*csi.ListVolumesResponse_Entry
	for _, d := range disks {
		slotInter, ok := d["slot"]
		if !ok {
			continue
		}
		slot, _ := slotInter.(string)
		if !strings.HasPrefix(slot, "pvc-") {
			continue
		}

		exportInter, _ := d["nvme-tcp-export"]
		export, _ := exportInter.(string)
		if export != "yes" {
			continue
		}

		sizeStr, _ := d["file-size"].(string)
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

	klog.V(4).Infof("GetCapacity requested")

	if cs.provider == ProviderStatic {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}

	disks, err := cs.restGet("/disk")
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

	exists, capBytes, err := cs.volumeExists(slot)
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
	for _, capability := range req.VolumeCapabilities {
		if capability.GetBlock() == nil && capability.GetMount() == nil {
			supported = false
		}
		if mode := capability.GetAccessMode(); mode != nil && mode.GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			supported = false
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
	if sourceVol == "" {
		return nil, status.Error(codes.InvalidArgument, "SourceVolumeId required")
	}
	if snapName == "" {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name required")
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
		return nil, status.Error(codes.InvalidArgument, "restURL required for dynamic provider")
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

	localExecute := func(cmd string) error {
		klog.V(5).Infof("Executing local backend command (snapshot): %s", cmd)
		body := map[string]string{"command": cmd}
		jsonBody, _ := json.Marshal(body)

		req, err := http.NewRequest("POST", localRestURL+"/execute", bytes.NewReader(jsonBody))
		if err != nil {
			return err
		}
		req.SetBasicAuth(localUsername, localPassword)
		req.Header.Set("Content-Type", "application/json")

		resp, err := cs.client.Do(req)
		if err != nil {
			return fmt.Errorf("HTTP request failed: %w", err)
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			return fmt.Errorf("execute error: status %d, body: %s", resp.StatusCode, string(respBody))
		}

		var result map[string]interface{}
		if json.Unmarshal(respBody, &result) == nil {
			if errMsg, ok := result["error"].(string); ok && errMsg != "" {
				return fmt.Errorf("command failed: %s", errMsg)
			}
		}
		return nil
	}

	if err := localExecute(fmt.Sprintf("/disk/btrfs/subvolume/add read-only=yes parent=%s fs=%s name=%s", sourceSubVol, backendDisk, snapSubVol)); err != nil {
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

	err := cs.executeCommand(fmt.Sprintf("/disk/btrfs/subvolume/remove [find name=%s]", snapSubVol))
	if err != nil && !strings.Contains(err.Error(), "no such item") {
		return nil, status.Errorf(codes.Internal, "Delete snapshot failed: %v", err)
	}

	klog.V(4).Infof("Snapshot %s deleted (or already gone)", snapSubVol)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(_ context.Context, _ *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	cs.initConfig()

	klog.V(4).Infof("ListSnapshots requested (placeholder implementation)")

	if cs.provider == ProviderStatic {
		return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{}}, nil
	}

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
