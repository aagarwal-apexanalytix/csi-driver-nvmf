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

	// Back-compat: allow deviceUUID/EUI to derive a stable deviceID when present
	if volOpts["deviceUUID"] != "" {
		if deviceID != "" {
			klog.Warningf("deviceUUID overwriting existing deviceID, volID=%s", volName)
		}
		deviceID = strings.Join([]string{"uuid", volOpts["deviceUUID"]}, ".")
	}
	if volOpts["deviceEUI"] != "" {
		if deviceID != "" {
			klog.Warningf("deviceEUI overwriting existing deviceID, volID=%s", volName)
		}
		deviceID = strings.Join([]string{"eui", volOpts["deviceEUI"]}, ".")
	}

	nqn := volOpts["nqn"]

	if targetTrAddr == "" || nqn == "" || targetTrPort == "" || targetTrType == "" || deviceID == "" {
		return nil, fmt.Errorf("some nvme target info is missing, volID=%s", volName)
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

// AttachDisk publishes the NVMe device as either raw block (bind-mount to target file)
// or filesystem (format+mount).
func AttachDisk(req *csi.NodePublishVolumeRequest, devicePath string) error {
	if req.GetVolumeCapability() == nil {
		return fmt.Errorf("volume capability missing in request")
	}

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return fmt.Errorf("targetPath missing in request")
	}

	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{Interface: mounter, Exec: exec.New()}

	// ===== RAW BLOCK VOLUME =====
	//
	// Best practice in Kubernetes CSI is to bind-mount the device node to targetPath
	// (targetPath is typically a FILE under kubelet volumeDevices).
	if req.GetVolumeCapability().GetBlock() != nil {
		klog.V(4).Infof("AttachDisk: raw block publish target=%s device=%s", targetPath, devicePath)

		// Ensure parent dir exists
		if err := os.MkdirAll(filepath.Dir(targetPath), 0750); err != nil {
			return fmt.Errorf("failed to create parent dir for %s: %w", targetPath, err)
		}

		// Ensure target exists as a file (kubelet usually passes a file path for block)
		// If something exists but isn't a regular file, clean it up.
		if fi, err := os.Lstat(targetPath); err == nil {
			if !fi.Mode().IsRegular() {
				// Could be stale symlink/dir/device etc.
				if rmErr := os.RemoveAll(targetPath); rmErr != nil {
					return fmt.Errorf("failed to remove non-regular targetPath %s: %w", targetPath, rmErr)
				}
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat targetPath %s: %w", targetPath, err)
		}

		// Create the file if missing
		f, err := os.OpenFile(targetPath, os.O_CREATE, 0640)
		if err != nil {
			return fmt.Errorf("failed to create target file %s: %w", targetPath, err)
		}
		_ = f.Close()

		// Idempotency: if already bind-mounted, return success
		notMounted, err := safeMounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			// If mount point check fails, continue cautiously (some environments behave oddly for file targets).
			klog.Warningf("AttachDisk: mountpoint check failed for %s: %v (continuing)", targetPath, err)
		} else if !notMounted {
			klog.V(4).Infof("AttachDisk: raw block already mounted at %s - skipping", targetPath)
			return nil
		}

		// Bind mount the device node onto the file path.
		// Note: "ro" is enforced at mount level for block device publication when requested.
		opts := []string{"bind"}
		if req.GetReadonly() {
			opts = append(opts, "ro")
		} else {
			opts = append(opts, "rw")
		}

		if err := safeMounter.Mount(devicePath, targetPath, "", opts); err != nil {
			return fmt.Errorf("failed to bind-mount raw block %s -> %s: %w", devicePath, targetPath, err)
		}

		klog.Infof("AttachDisk: published raw block via bind mount %s -> %s", devicePath, targetPath)
		return nil
	}

	// ===== FILESYSTEM VOLUME =====
	if mountCap := req.GetVolumeCapability().GetMount(); mountCap != nil {
		klog.V(4).Infof("AttachDisk: filesystem publish target=%s device=%s", targetPath, devicePath)

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
			klog.Infof("AttachDisk: %s already mounted - skipping", targetPath)
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
			klog.Warningf("AttachDisk: fstype=%q not explicitly supported by online resize logic (vol=%s)", fsType, req.GetVolumeId())
		}

		opts := append([]string{}, mountCap.GetMountFlags()...)
		if req.GetReadonly() {
			opts = append(opts, "ro")
		} else {
			opts = append(opts, "rw")
		}

		if err := safeMounter.FormatAndMount(devicePath, targetPath, fsType, opts); err != nil {
			return fmt.Errorf("failed to format+mount %s to %s (%s): %w", devicePath, targetPath, fsType, err)
		}

		klog.Infof("AttachDisk: mounted filesystem %s to %s (%s)", devicePath, targetPath, fsType)
		return nil
	}

	return fmt.Errorf("unsupported volume capability type")
}

// DetachDisk unpublishes the volume (handles both raw block bind-mount and filesystem mount).
func DetachDisk(targetPath string) error {
	if targetPath == "" {
		return fmt.Errorf("targetPath is required")
	}

	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec.New(),
	}

	// If it is mounted (file bind mount or directory mount), unmount it.
	isNotMountPoint, err := safeMounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("DetachDisk: mountpoint check failed for %s: %v (continuing best-effort)", targetPath, err)
		}
	} else if !isNotMountPoint {
		if umErr := safeMounter.Unmount(targetPath); umErr != nil {
			// best-effort: return error (so kubelet retries), but still try cleanup
			klog.Errorf("DetachDisk: failed to unmount %s: %v", targetPath, umErr)
			// continue cleanup attempts below
		} else {
			klog.V(4).Infof("DetachDisk: unmounted %s", targetPath)
		}
	}

	// Remove targetPath (for block this is a file; for fs it is a dir).
	// RemoveAll is safe here for idempotency.
	if rmErr := os.RemoveAll(targetPath); rmErr != nil && !os.IsNotExist(rmErr) {
		klog.Warningf("DetachDisk: failed to remove targetPath %s: %v", targetPath, rmErr)
	}

	// Remove the parent publish dir if empty (best-effort).
	parentDir := filepath.Dir(targetPath)
	if err := os.Remove(parentDir); err != nil && !os.IsNotExist(err) {
		klog.V(5).Infof("DetachDisk: parent dir %s not removed (likely not empty): %v", parentDir, err)
	}

	klog.V(4).Infof("DetachDisk: cleanup completed for %s", targetPath)
	return nil
}
