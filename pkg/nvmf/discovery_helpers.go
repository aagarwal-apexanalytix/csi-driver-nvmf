package nvmf

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

func (cs *ControllerServer) diskListPath() string {
	if cs.provider == ProviderNvmeProxy {
		// nvme-proxy README typically shows trailing slash
		return "/disk/"
	}
	return "/disk"
}

func parseSizeToBytes(s string) int64 {
	if s == "" {
		return 0
	}
	s = strings.TrimSpace(strings.ToUpper(s))
	s = strings.TrimSuffix(s, "B")
	s = strings.TrimSuffix(s, "GIB")
	s = strings.TrimSuffix(s, "GI")
	s = strings.TrimSuffix(s, "G")

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		klog.Warningf("Failed to parse size string %q: %v", s, err)
		return 0
	}
	return int64(f * 1024 * 1024 * 1024)
}

func parseAnySizeBytes(d map[string]interface{}) int64 {
	// common string keys
	for _, k := range []string{"file-size", "size", "fileSize", "file_size", "fileSizeGiB"} {
		if s, ok := d[k].(string); ok && s != "" {
			if b := parseSizeToBytes(s); b > 0 {
				return b
			}
		}
	}

	// common numeric keys
	for _, k := range []string{"sizeBytes", "fileSizeBytes", "bytes"} {
		switch v := d[k].(type) {
		case float64:
			if v > 0 {
				return int64(v)
			}
		case int64:
			if v > 0 {
				return v
			}
		case json.Number:
			if i, err := v.Int64(); err == nil && i > 0 {
				return i
			}
		}
	}
	return 0
}

// Find disk identifier.
// - MikroTik: prefer ".id" (or "id") when present
// - nvme-proxy: works purely by slot; return slot
func (cs *ControllerServer) getDiskID(slot, restURL, username, password string) (string, error) {
	disks, err := cs.restGet(cs.diskListPath(), restURL, username, password)
	if err != nil {
		return "", err
	}
	for _, d := range disks {
		// slot may be string
		if s, ok := d["slot"].(string); ok && s == slot {
			// MikroTik returns ".id"
			if id, ok := d[".id"].(string); ok && id != "" {
				return id, nil
			}
			// some APIs return "id"
			if id, ok := d["id"].(string); ok && id != "" {
				return id, nil
			}
			// nvme-proxy patches/deletes by slot
			return slot, nil
		}
	}
	return "", fmt.Errorf("disk with slot %s not found", slot)
}

func (cs *ControllerServer) volumeExists(volumeID, restURL, username, password string) (bool, int64, error) {
	disks, err := cs.restGet(cs.diskListPath(), restURL, username, password)
	if err != nil {
		return false, 0, err
	}
	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == volumeID {
			return true, parseAnySizeBytes(d), nil
		}
	}
	return false, 0, nil
}

// Utility used by ListVolumes
func sortVolumesByID(vols []string) {
	sort.Slice(vols, func(i, j int) bool { return vols[i] < vols[j] })
}
