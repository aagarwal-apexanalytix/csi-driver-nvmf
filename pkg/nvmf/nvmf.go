/* Copyright 2021 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package nvmf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"
)

type nvmfDiskInfo struct {
	VolName   string
	Nqn       string
	Addr      string
	Port      string
	DeviceID  string
	Transport string
	HostId    string
	HostNqn   string
}

// getNVMfDiskInfo extracts NVMe-oF connection parameters from the volume context.
func getNVMfDiskInfo(req *csi.NodePublishVolumeRequest) (*nvmfDiskInfo, error) {
	volName := req.GetVolumeId()
	volOpts := req.GetVolumeContext()
	targetTrAddr := volOpts["targetTrAddr"]
	targetTrPort := volOpts["targetTrPort"]
	targetTrType := volOpts["targetTrType"]
	devHostNqn := volOpts["hostNqn"]
	devHostId := volOpts["hostId"]
	deviceID := volOpts["deviceID"]

	if volOpts["deviceUUID"] != "" {
		if deviceID != "" {
			klog.Warningf("Warning: deviceUUID is overwriting already defined deviceID, volID: %s ", volName)
		}
		deviceID = strings.Join([]string{"uuid", volOpts["deviceUUID"]}, ".")
	}
	if volOpts["deviceEUI"] != "" {
		if deviceID != "" {
			klog.Warningf("Warning: deviceEUI is overwriting already defined deviceID, volID: %s ", volName)
		}
		deviceID = strings.Join([]string{"eui", volOpts["deviceEUI"]}, ".")
	}

	nqn := volOpts["nqn"]

	if targetTrAddr == "" || nqn == "" || targetTrPort == "" || targetTrType == "" || deviceID == "" {
		return nil, fmt.Errorf("some nvme target info is missing, volID: %s ", volName)
	}

	return &nvmfDiskInfo{
		VolName:   volName,
		Addr:      targetTrAddr,
		Port:      targetTrPort,
		Nqn:       nqn,
		DeviceID:  deviceID,
		Transport: targetTrType,
		HostNqn:   devHostNqn,
		HostId:    devHostId,
	}, nil
}

// AttachDisk handles publishing the NVMe device (raw block or formatted filesystem).
// AttachDisk handles publishing the NVMe device (raw block or formatted filesystem).
func AttachDisk(req *csi.NodePublishVolumeRequest, devicePath string) error {
	targetPath := req.GetTargetPath()
	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{Interface: mounter, Exec: exec.New()}

	if req.GetVolumeCapability() == nil {
		return fmt.Errorf("volume capability missing in request")
	}

	// ===== RAW BLOCK VOLUME =====
	if block := req.GetVolumeCapability().GetBlock(); block != nil {
		klog.V(4).Infof("AttachDisk: handling raw block volume at target %s (device %s)", targetPath, devicePath)

		// Idempotency check: already published?
		if fi, err := os.Lstat(targetPath); err == nil {
			// Exists as symlink, device, or something usable → assume good
			if (fi.Mode()&os.ModeSymlink) != 0 || (fi.Mode()&os.ModeDevice) != 0 {
				klog.V(4).Infof("Raw block already published at %s (symlink/device exists) - skipping", targetPath)
				return nil
			}
			// If it's a regular file or directory (stale?), we'll clean it up below
		}

		// Clean up any stale target (best-effort)
		if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
			klog.Warningf("Failed to remove stale target %s: %v", targetPath, err)
		}

		// Create parent directory if needed
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("failed to create parent dir for %s: %w", targetPath, err)
		}

		// Create symlink to the real NVMe device
		if err := os.Symlink(devicePath, targetPath); err != nil {
			return fmt.Errorf("failed to create symlink %s -> %s: %w", targetPath, devicePath, err)
		}

		klog.Infof("AttachDisk: Successfully published raw block via symlink %s -> %s", targetPath, devicePath)
		return nil
	}

	// ===== FILESYSTEM VOLUME =====
	if mountCap := req.GetVolumeCapability().GetMount(); mountCap != nil {
		klog.V(4).Infof("AttachDisk: handling filesystem volume at target %s (device %s)", targetPath, devicePath)

		notMounted, err := safeMounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			if os.IsNotExist(err) {
				if err = os.MkdirAll(targetPath, 0750); err != nil {
					return fmt.Errorf("failed to create target path %s: %w", targetPath, err)
				}
				notMounted = true
			} else {
				return fmt.Errorf("cannot check if %s is mount point: %w", targetPath, err)
			}
		}

		if !notMounted {
			klog.Infof("AttachDisk: %s is already mounted - skipping", targetPath)
			return nil
		}

		fsType := req.VolumeContext["csi.storage.k8s.io/fstype"]
		if fsType == "" {
			fsType = mountCap.GetFsType()
		}
		if fsType == "" {
			fsType = "ext4"
		}

		if fsType != "ext4" && fsType != "xfs" {
			klog.Warningf("Unsupported fstype %q for volume %s (online resize may fail)", fsType, req.GetVolumeId())
		}

		mountOptions := mountCap.GetMountFlags()
		options := append([]string{}, mountOptions...)
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}

		if err = safeMounter.FormatAndMount(devicePath, targetPath, fsType, options); err != nil {
			return fmt.Errorf("failed to format and mount %s to %s (%s): %w", devicePath, targetPath, fsType, err)
		}

		klog.Infof("AttachDisk: Successfully mounted filesystem %s to %s (%s)", devicePath, targetPath, fsType)
		return nil
	}

	return fmt.Errorf("unsupported volume capability type")
}

// DetachDisk performs unmount and cleanup of the target path (handles both block and filesystem cases).
func DetachDisk(targetPath string) error {
	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec.New(),
	}

	// Stat the target path
	fi, err := os.Lstat(targetPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("DetachDisk: failed to stat %s: %v", targetPath, err)
		}
		// If not exist → just try to clean parent dir
	} else {
		// --- Symlink case (raw block) ---
		if (fi.Mode() & os.ModeSymlink) != 0 {
			klog.V(4).Infof("DetachDisk: removing symlink %s", targetPath)
			if rmErr := os.Remove(targetPath); rmErr != nil {
				klog.Warningf("DetachDisk: failed to remove symlink %s: %v", targetPath, rmErr)
			}
			// Symlink handled → proceed directly to parent cleanup
		} else {
			// --- Filesystem mount case ---
			isNotMountPoint, checkErr := safeMounter.IsLikelyNotMountPoint(targetPath)
			if checkErr != nil {
				if !os.IsNotExist(checkErr) {
					klog.Warningf("DetachDisk: failed to check mount point %s: %v", targetPath, checkErr)
				}
			} else if !isNotMountPoint {
				if umErr := safeMounter.Unmount(targetPath); umErr != nil {
					klog.Errorf("DetachDisk: failed to unmount %s: %v", targetPath, umErr)
					// Continue even on failure (best-effort)
				} else {
					klog.V(4).Infof("DetachDisk: successfully unmounted filesystem %s", targetPath)
				}
			} else {
				klog.V(4).Infof("DetachDisk: %s is not a mount point - no unmount needed", targetPath)
			}
		}
	}

	// Common cleanup: remove parent directory if empty
	parentDir := filepath.Dir(targetPath)
	if err := os.Remove(parentDir); err != nil && !os.IsNotExist(err) {
		klog.V(5).Infof("DetachDisk: publish directory %s not removed (may not be empty): %v", parentDir, err)
	}

	klog.V(4).Infof("DetachDisk: cleanup completed for %s", targetPath)
	return nil
}
