/* Copyright 2021 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations
under the License.
*/

package nvmf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
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
	csi.UnimplementedControllerServer
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
	klog.V(4).Infof("ControllerServer initialized: provider=%s, backendDisk=%s, backendMount=%s, restURL=%s", cs.provider, cs.backendDisk, cs.backendMount, cs.restURL)
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

func getEffectiveCompress(params map[string]string) string {
	if val, ok := params["compression"]; ok {
		val = strings.ToLower(strings.TrimSpace(val))
		switch val {
		case "off", "none", "no", "false", "disabled":
			return "no"
		case "on", "yes", "true", "enabled", "zstd", "zstd:1", "zstd:2", "zstd:3", "zstd:4",
			"zstd:5", "zstd:6", "zstd:7", "zstd:8", "zstd:9", "zstd:10", "zstd:11",
			"zstd:12", "zstd:13", "zstd:14", "zstd:15", "lzo", "zlib":
			return "yes"
		default:
			klog.Warningf("Invalid compression value %q → defaulting to compress=no", val)
		}
	}

	klog.V(4).Infof("No compression parameter specified → defaulting to compress=no")
	return "no"
}

// Generic REST helper (takes credentials per call for per-SC support)
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

// Helper wrappers (take full URL and credentials)
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

	contentSource := req.GetVolumeContentSource()

	// Extract parameters early
	params := req.Parameters

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

	// Resolve per-SC credentials
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
	if localUsername == "" {
		return nil, status.Error(codes.InvalidArgument, "username required (no global configured)")
	}

	localPassword := params["password"]
	if localPassword == "" {
		localPassword = cs.password
	}
	if localPassword == "" {
		return nil, status.Error(codes.InvalidArgument, "password required (no global configured)")
	}

	// Handle content source (clone or restore from snapshot)
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

	// Base comment (using effective fstype)
	comment := fmt.Sprintf("k8s-pvc:%s:%s/%s (pv:%s) fstype:%s%s", clusterName, pvcNamespace, pvcHumanName, pvName, fstype, cloneInfo)

	// Volume ID / slot
	volumeID := strings.ReplaceAll(strings.ToLower(pvName), "-", "")
	slot := volumeID
	subVolName := "vol-" + volumeID
	imgPath := backendMount + "/" + subVolName + "/volume.img"

	// Determine effective capacity
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

	// Round up to full GiB
	giBFloat := math.Ceil(float64(effectiveBytes) / (1024.0 * 1024.0 * 1024.0))
	sizeGiB := fmt.Sprintf("%dG", int64(giBFloat))
	actualBytes := int64(giBFloat) * 1024 * 1024 * 1024

	// Static provider handling
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
					"targetTrPort":              targetPort,
					"targetTrType":              "tcp",
					"nqn":                       slot,
					"deviceID":                  slot,
					"csi.storage.k8s.io/fstype": fstype,
				},
			},
		}, nil
	}

	// Idempotency check
	exists, currentSize, err := cs.volumeExists(volumeID, localRestURL, localUsername, localPassword)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to check existing volume: %v", err)
	}
	if exists {
		if currentSize >= actualBytes {
			// Best-effort update comment
			if diskID, getErr := cs.getDiskID(slot, localRestURL, localUsername, localPassword); getErr == nil {
				commentData := map[string]string{
					"numbers": diskID,
					"comment": comment,
				}
				if setErr := cs.restPost("/disk/set", commentData, localRestURL, localUsername, localPassword); setErr != nil {
					klog.Warningf("Failed to update comment on existing disk: %v", setErr)
				}
			}
			return &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      volumeID,
					CapacityBytes: currentSize,
					VolumeContext: map[string]string{
						"targetTrAddr":              targetAddr,
						"targetTrPort":              targetPort,
						"targetTrType":              "tcp",
						"nqn":                       slot,
						"deviceID":                  slot,
						"csi.storage.k8s.io/fstype": fstype,
					},
					ContentSource: contentSource,
				},
			}, nil
		}
		return nil, status.Error(codes.AlreadyExists, "Volume exists with smaller size than requested")
	}

	// Provision new volume
	klog.V(4).Infof("Provisioning new volume %s (slot=%s, size=%s)%s", req.Name, slot, sizeGiB, cloneInfo)

	// Create BTRFS subvolume (empty or snapshot)
	subvolData := map[string]string{
		"fs":   backendDisk,
		"name": subVolName,
	}
	if parentSubvol != "" {
		subvolData["parent"] = parentSubvol
		subvolData["read-only"] = "no" // Ensure RW for clone
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

	// Enable export
	diskID, err := cs.getDiskID(slot, localRestURL, localUsername, localPassword)
	if err != nil {
		_ = cs.restDelete("/disk/"+slot, localRestURL, localUsername, localPassword)
		_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
		return nil, status.Errorf(codes.Internal, "Failed to retrieve disk .id for export: %v", err)
	}

	exportData := map[string]string{
		"nvme-tcp-export": "yes",
		"nvme-tcp-port":   targetPort,
	}
	err = cs.restPatch("/disk/"+diskID, exportData, localRestURL, localUsername, localPassword)
	if err != nil {
		klog.Warningf("PATCH export failed, trying SET: %v", err)
		setData := map[string]string{
			"numbers":         diskID,
			"nvme-tcp-export": "yes",
			"nvme-tcp-port":   targetPort,
		}
		if err2 := cs.restPost("/disk/set", setData, localRestURL, localUsername, localPassword); err2 != nil {
			_ = cs.restDelete("/disk/"+diskID, localRestURL, localUsername, localPassword)
			_ = cs.restDelete("/disk/btrfs/subvolume/"+subVolName, localRestURL, localUsername, localPassword)
			return nil, status.Errorf(codes.Internal, "Export enable failed: %v / %v", err, err2)
		}
	}

	compressValue := getEffectiveCompress(params)
	if compressValue != "" {
		setData := map[string]string{
			"numbers":  diskID,
			"compress": compressValue,
		}
		err = cs.restPost("/disk/set", setData, localRestURL, localUsername, localPassword)
		if err != nil {
			klog.Warningf("Failed to set compress=%s on disk %s (requested: %s): %v (continuing anyway)",
				compressValue, diskID, params["compression"], err)
		} else {
			klog.Infof("Successfully set compress=%s on disk %s for volume %s (requested: %s)",
				compressValue, diskID, volumeID, params["compression"])
		}

		// Record in comment for visibility in /disk print
		comment += fmt.Sprintf(" compress:%s", compressValue)
	}

	// Set comment
	commentData := map[string]string{
		"numbers": diskID,
		"comment": comment,
	}
	if setErr := cs.restPost("/disk/set", commentData, localRestURL, localUsername, localPassword); setErr != nil {
		klog.Warningf("Failed to set comment (non-critical): %v", setErr)
	} else {
		klog.V(4).Infof("Added comment: %s", comment)
	}

	klog.V(4).Infof("Successfully created volume %s (actual size %d bytes)%s", volumeID, actualBytes, cloneInfo)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: actualBytes,
			VolumeContext: map[string]string{
				"targetTrAddr":              targetAddr,
				"targetTrPort":              targetPort,
				"targetTrType":              "tcp",
				"nqn":                       slot,
				"deviceID":                  slot,
				"csi.storage.k8s.io/fstype": fstype,
				"compression":               compressValue,
			},
			ContentSource: contentSource,
		},
	}, nil
}

func (cs *ControllerServer) getSourceFstype(sourceVolID string, restURL string, username string, password string) (string, error) {
	disks, err := cs.restGet("/disk", restURL, username, password)
	if err != nil {
		return "", err
	}
	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == sourceVolID {
			commentStr, ok := d["comment"].(string)
			if !ok || commentStr == "" {
				return "", fmt.Errorf("no comment found for source volume %s", sourceVolID)
			}
			// Parse fstype from comment, assuming format "... fstype:xxx"
			fstypePrefix := " fstype:"
			prefixIdx := strings.LastIndex(commentStr, fstypePrefix)
			if prefixIdx == -1 {
				return "", fmt.Errorf("fstype not found in comment for source volume %s", sourceVolID)
			}
			fstypeStr := commentStr[prefixIdx+len(fstypePrefix):]
			fstypeParts := strings.SplitN(fstypeStr, " ", 2)
			if len(fstypeParts) == 0 {
				return "", fmt.Errorf("no fstype value found in comment for source volume %s", sourceVolID)
			}
			fstype := strings.TrimSpace(fstypeParts[0])
			return fstype, nil
		}
	}
	return "", fmt.Errorf("source volume %s not found", sourceVolID)
}

func (cs *ControllerServer) handleContentSource(contentSource *csi.VolumeContentSource, localRestURL, localUsername, localPassword string, fstype *string) (string, int64, string, error) {
	if contentSource == nil {
		return "", 0, "", nil
	}
	var parentSubvol string
	var sourceCapacity int64
	var cloneInfo string
	var sourceVolID string
	// Snapshot source
	if snapSrc := contentSource.GetSnapshot(); snapSrc != nil {
		snapID := snapSrc.GetSnapshotId()
		if snapID == "" {
			return "", 0, "", status.Error(codes.InvalidArgument, "Snapshot ID missing")
		}
		parentSubvol = snapID
		cloneInfo = fmt.Sprintf(" restored-from-snapshot:%s", snapID)
		if !strings.HasPrefix(snapID, "snap-") {
			return "", 0, "", status.Error(codes.InvalidArgument, "Invalid snapshot ID format")
		}
		temp := strings.TrimPrefix(snapID, "snap-")
		lastDash := strings.LastIndex(temp, "-")
		if lastDash == -1 {
			return "", 0, "", status.Error(codes.InvalidArgument, "Invalid snapshot ID format (missing source volume)")
		}
		sourceVolID = temp[lastDash+1:]
		exists, size, err := cs.volumeExists(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			return "", 0, "", status.Errorf(codes.Internal, "Failed to query source volume size: %v", err)
		}
		if !exists {
			return "", 0, "", status.Error(codes.NotFound, "Source volume for snapshot not found")
		}
		sourceCapacity = size
		// Override fstype with source's if different
		sourceFstype, err := cs.getSourceFstype(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			klog.Warningf("Failed to get source fstype for restore: %v", err)
		} else if sourceFstype != "" && sourceFstype != *fstype {
			*fstype = sourceFstype
			klog.V(4).Infof("Overrode fstype with source's value for restore: %s", *fstype)
		}
		// Volume source (clone)
	} else if volSrc := contentSource.GetVolume(); volSrc != nil {
		sourceVolID = volSrc.GetVolumeId()
		if sourceVolID == "" {
			return "", 0, "", status.Error(codes.InvalidArgument, "Source Volume ID missing")
		}
		parentSubvol = "vol-" + sourceVolID
		cloneInfo = fmt.Sprintf(" cloned-from-volume:%s", sourceVolID)
		exists, size, err := cs.volumeExists(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			return "", 0, "", status.Errorf(codes.Internal, "Failed to query source volume: %v", err)
		}
		if !exists {
			return "", 0, "", status.Error(codes.NotFound, "Source volume not found")
		}
		sourceCapacity = size
		// Override fstype with source's if different
		sourceFstype, err := cs.getSourceFstype(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			klog.Warningf("Failed to get source fstype for clone: %v", err)
		} else if sourceFstype != "" && sourceFstype != *fstype {
			*fstype = sourceFstype
			klog.V(4).Infof("Overrode fstype with source's value for clone: %s", *fstype)
		}
		// Unknown / invalid
	} else {
		return "", 0, "", status.Error(codes.Unimplemented, "Unknown or unsupported volume content source type")
	}
	return parentSubvol, sourceCapacity, cloneInfo, nil
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
	// Use global credentials for delete (no per-volume context)
	diskID, _ := cs.getDiskID(slot, cs.restURL, cs.username, cs.password)
	if diskID != "" {
		_ = cs.restPatch("/disk/"+diskID, map[string]string{"nvme-tcp-export": "no"}, cs.restURL, cs.username, cs.password)
		_ = cs.restDelete("/disk/"+diskID, cs.restURL, cs.username, cs.password)
	} else {
		_ = cs.restPatch("/disk/"+slot, map[string]string{"nvme-tcp-export": "no"}, cs.restURL, cs.username, cs.password)
		_ = cs.restDelete("/disk/"+slot, cs.restURL, cs.username, cs.password)
	}
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
	// Round up to the next full GiB
	giBFloat := math.Ceil(float64(newBytes) / (1024.0 * 1024.0 * 1024.0))
	newGiB := fmt.Sprintf("%dG", int64(giBFloat))
	actualBytes := int64(giBFloat) * 1024 * 1024 * 1024
	// Use global credentials
	diskID, err := cs.getDiskID(slot, cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get disk .id for expansion: %v", err)
	}
	err = cs.restPatch("/disk/"+diskID, map[string]string{"file-size": newGiB}, cs.restURL, cs.username, cs.password)
	if err != nil {
		if err2 := cs.restPost("/disk/set", map[string]string{
			"numbers":   diskID,
			"file-size": newGiB,
		}, cs.restURL, cs.username, cs.password); err2 != nil {
			return nil, status.Errorf(codes.Internal, "Expand failed (PATCH .id: %v, SET .id: %v)", err, err2)
		}
	}
	klog.V(4).Infof("Successfully expanded volume %s to %d bytes", slot, actualBytes)
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         actualBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (cs *ControllerServer) ListVolumes(_ context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	cs.initConfig()
	if cs.provider == ProviderStatic {
		// Static mode: no managed volumes to list
		return &csi.ListVolumesResponse{Entries: []*csi.ListVolumesResponse_Entry{}}, nil
	}
	disks, err := cs.restGet("/disk", cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "List disks failed: %v", err)
	}
	// Collect all exported volumes
	var volumes []*csi.Volume
	for _, d := range disks {
		slot, _ := d["slot"].(string)
		if slot == "" {
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
		volumes = append(volumes, &csi.Volume{
			VolumeId:      slot,
			CapacityBytes: capBytes,
		})
	}
	// Sort by VolumeId for deterministic, stable pagination
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].VolumeId < volumes[j].VolumeId
	})
	// Pagination handling
	startIdx := 0
	if token := req.GetStartingToken(); token != "" {
		if i, err := strconv.Atoi(token); err == nil && i >= 0 && i < len(volumes) {
			startIdx = i
		} else {
			return nil, status.Errorf(codes.Aborted, "invalid starting token %q", token)
		}
	}
	maxEntries := int(req.GetMaxEntries())
	endIdx := len(volumes)
	if maxEntries > 0 && startIdx+maxEntries < endIdx {
		endIdx = startIdx + maxEntries
	}
	entries := make([]*csi.ListVolumesResponse_Entry, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: volumes[i],
		})
	}
	nextToken := ""
	if endIdx < len(volumes) {
		nextToken = strconv.Itoa(endIdx)
	}
	klog.V(4).Infof("ListVolumes returning %d entries (total exported volumes: %d, nextToken: %s)", len(entries), len(volumes), nextToken)
	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
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
	for _, capability := range req.VolumeCapabilities {
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

func (cs *ControllerServer) GetSnapshot(ctx context.Context, req *csi.GetSnapshotRequest) (*csi.GetSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID is required")
	}
	listReq := &csi.ListSnapshotsRequest{
		SnapshotId: req.GetSnapshotId(),
	}
	listResp, err := cs.ListSnapshots(ctx, listReq)
	if err != nil {
		return nil, err
	}
	if len(listResp.Entries) == 0 {
		return nil, status.Error(codes.NotFound, "Snapshot not found")
	}
	if len(listResp.Entries) > 1 {
		klog.Warningf("GetSnapshot found multiple entries for ID %s", req.GetSnapshotId())
	}
	return &csi.GetSnapshotResponse{
		Snapshot: listResp.Entries[0].Snapshot,
	}, nil
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
	// Per-SC credentials with fallback
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
	if localUsername == "" {
		return nil, status.Error(codes.InvalidArgument, "username required (no global configured)")
	}
	localPassword := params["password"]
	if localPassword == "" {
		localPassword = cs.password
	}
	if localPassword == "" {
		return nil, status.Error(codes.InvalidArgument, "password required (no global configured)")
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
	// Size from source volume using same credentials
	_, sourceSize, err := cs.volumeExists(sourceVol, localRestURL, localUsername, localPassword)
	if err != nil {
		klog.Warningf("Failed to retrieve source volume size for snapshot %s: %v", snapSubVol, err)
		sourceSize = 0
	}
	klog.V(4).Infof("Successfully created snapshot %s from volume %s (size %d bytes)", snapSubVol, sourceVol, sourceSize)
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
	snapSubVol := req.SnapshotId
	if snapSubVol == "" {
		return nil, status.Error(codes.InvalidArgument, "SnapshotId required")
	}
	klog.V(4).Infof("DeleteSnapshot requested for snapshot %s", snapSubVol)
	if cs.provider == ProviderStatic {
		return &csi.DeleteSnapshotResponse{}, nil
	}
	// Use global credentials (no per-snapshot context)
	_ = cs.restDelete("/disk/btrfs/subvolume/"+snapSubVol, cs.restURL, cs.username, cs.password)
	klog.V(4).Infof("Snapshot %s deleted (best-effort)", snapSubVol)
	return &csi.DeleteSnapshotResponse{}, nil
}

func (cs *ControllerServer) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	cs.initConfig()
	if cs.provider == ProviderStatic {
		return &csi.ListSnapshotsResponse{}, nil
	}
	// Use global credentials
	subvols, err := cs.restGet("/disk/btrfs/subvolume", cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list BTRFS subvolumes: %v", err)
	}
	var matchingSnapshots []*csi.Snapshot
	for _, sv := range subvols {
		nameIfc, ok := sv["name"]
		if !ok {
			continue
		}
		name := nameIfc.(string)
		if !strings.HasPrefix(name, "snap-") {
			continue
		}
		readOnlyIfc, _ := sv["read-only"]
		readOnlyStr := ""
		if readOnlyIfc != nil {
			readOnlyStr = readOnlyIfc.(string)
		}
		if readOnlyStr != "yes" {
			continue
		}
		parentIfc, ok := sv["parent"]
		if !ok {
			continue
		}
		parent := parentIfc.(string)
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
		// Consistent SizeBytes using global credentials
		_, sourceSize, err := cs.volumeExists(sourceVol, cs.restURL, cs.username, cs.password)
		if err != nil {
			klog.Warningf("Failed to get size for source volume %s of snapshot %s: %v", sourceVol, name, err)
			sourceSize = 0
		}
		snap := &csi.Snapshot{
			SnapshotId:     name,
			SourceVolumeId: sourceVol,
			ReadyToUse:     true,
			SizeBytes:      sourceSize,
		}
		matchingSnapshots = append(matchingSnapshots, snap)
	}
	// Pagination
	startIdx := 0
	if token := req.GetStartingToken(); token != "" {
		if i, err := strconv.Atoi(token); err == nil && i >= 0 && i < len(matchingSnapshots) {
			startIdx = i
		}
	}
	maxEntries := 0
	if req.MaxEntries > 0 {
		maxEntries = int(req.MaxEntries)
	}
	endIdx := len(matchingSnapshots)
	if maxEntries > 0 && startIdx+maxEntries < endIdx {
		endIdx = startIdx + maxEntries
	}
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx; i++ {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: matchingSnapshots[i],
		})
	}
	nextToken := ""
	if endIdx < len(matchingSnapshots) {
		nextToken = strconv.Itoa(endIdx)
	}
	klog.V(4).Infof("ListSnapshots returning %d entries (total matching: %d)", len(entries), len(matchingSnapshots))
	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(5).Infof("ControllerGetCapabilities requested")
	var caps []*csi.ControllerServiceCapability
	add := func(cap csi.ControllerServiceCapability_RPC_Type) {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
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
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumeID := req.GetVolumeId()
	if cs.provider == ProviderStatic {
		return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not supported in static provider mode")
	}
	mutableParams := req.GetMutableParameters()
	if len(mutableParams) == 0 {
		klog.V(4).Infof("ControllerModifyVolume no-op success for volume %s (no mutable parameters provided)", volumeID)
		return &csi.ControllerModifyVolumeResponse{}, nil
	}
	klog.V(4).Infof("ControllerModifyVolume requested for volume %s with mutable parameters: %v", volumeID, mutableParams)
	slot := volumeID
	diskID, err := cs.getDiskID(slot, cs.restURL, cs.username, cs.password)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Failed to locate disk for volume %s: %v", volumeID, err)
	}
	updated := false
	for key, value := range mutableParams {
		switch key {
		case "comment":
			commentData := map[string]string{
				"numbers": diskID,
				"comment": value,
			}
			if err := cs.restPost("/disk/set", commentData, cs.restURL, cs.username, cs.password); err != nil {
				return nil, status.Errorf(codes.Internal, "Failed to update comment on disk %s: %v", diskID, err)
			}
			klog.V(4).Infof("Successfully updated comment on volume %s to: %s", volumeID, value)
			updated = true
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported mutable parameter %q (only 'comment' is currently supported)", key)
		}
	}
	if !updated {
		klog.V(4).Infof("No supported mutable parameters were applied for volume %s", volumeID)
	}
	return &csi.ControllerModifyVolumeResponse{}, nil
}
