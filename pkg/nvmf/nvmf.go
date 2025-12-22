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

const (
	blockRawFileName = "volume"
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
// This is used for all volumes (only NVMe-oF provisioned volumes are supported).
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
func AttachDisk(req *csi.NodePublishVolumeRequest, devicePath string) error {
	targetPath := req.GetTargetPath()
	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec.New(),
	}

	if req.GetVolumeCapability() == nil {
		return fmt.Errorf("volume capability missing in request")
	}

	if block := req.GetVolumeCapability().GetBlock(); block != nil {
		// ==== Raw block volume ====
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			return fmt.Errorf("failed to create publish directory %s: %w", targetPath, err)
		}

		blockFile := filepath.Join(targetPath, blockRawFileName)

		// Check if already published correctly (bind-mount exists and is a device)
		if fi, err := os.Lstat(blockFile); err == nil {
			if (fi.Mode() & os.ModeDevice) != 0 {
				klog.V(4).Infof("AttachDisk: raw block already published correctly at %s", blockFile)
				return nil
			}
		}

		// Create placeholder file if missing
		if _, err := os.Stat(blockFile); os.IsNotExist(err) {
			f, createErr := os.OpenFile(blockFile, os.O_CREATE|os.O_WRONLY, 0660)
			if createErr != nil {
				return fmt.Errorf("failed to create block placeholder file %s: %w", blockFile, createErr)
			}
			f.Close()
		}

		// Bind-mount the device to the placeholder file
		mountOptions := []string{"bind"}
		if req.GetReadonly() {
			mountOptions = append(mountOptions, "ro")
		}
		if err := mounter.Mount(devicePath, blockFile, "", mountOptions); err != nil {
			// Cleanup placeholder on failure
			_ = os.Remove(blockFile)
			return fmt.Errorf("failed to bind-mount device %s to %s: %w", devicePath, blockFile, err)
		}

		klog.Infof("AttachDisk: Successfully published raw block device %s to %s", devicePath, blockFile)
		return nil
	}

	if mountCap := req.GetVolumeCapability().GetMount(); mountCap != nil {
		// ==== Filesystem volume ====
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
			klog.Infof("AttachDisk: %s is already mounted", targetPath)
			return nil
		}

		fsType := mountCap.GetFsType()
		if fsType == "" {
			fsType = "ext4" // default if not specified
		}
		mountOptions := mountCap.GetMountFlags()
		options := append([]string{}, mountOptions...)
		if req.GetReadonly() {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}

		if err = safeMounter.FormatAndMount(devicePath, targetPath, fsType, options); err != nil {
			return fmt.Errorf("failed to format and mount device %s to %s: %w", devicePath, targetPath, err)
		}

		klog.Infof("AttachDisk: Successfully mounted filesystem device %s to %s", devicePath, targetPath)
		return nil
	}

	return fmt.Errorf("unsupported volume capability")
}

// DetachDisk performs unmount and cleanup of the target path (handles both block and filesystem cases).
func DetachDisk(targetPath string) error {
	mounter := mount.New("")
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec.New(),
	}

	// Handle raw block case
	blockFile := filepath.Join(targetPath, blockRawFileName)
	if _, err := os.Lstat(blockFile); err == nil {
		// Placeholder file exists
		isNotMountPoint, checkErr := mounter.IsLikelyNotMountPoint(blockFile)
		if checkErr == nil && !isNotMountPoint {
			// It is mounted → unmount the bind
			if umErr := mounter.Unmount(blockFile); umErr != nil {
				klog.Errorf("failed to unmount block file %s: %v", blockFile, umErr)
				// Continue to cleanup anyway (best-effort)
			}
		}

		// Remove the placeholder file
		if rmErr := os.Remove(blockFile); rmErr != nil {
			klog.Errorf("failed to remove block file %s: %v", blockFile, rmErr)
		}
	}

	// Handle filesystem case
	if isNotMountPoint, err := safeMounter.IsLikelyNotMountPoint(targetPath); err == nil && !isNotMountPoint {
		if umErr := safeMounter.Unmount(targetPath); umErr != nil {
			klog.Errorf("failed to unmount filesystem target %s: %v", targetPath, umErr)
			// Continue to cleanup (best-effort)
		}
	}

	// Remove the publish directory (safe if already gone)
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		klog.Errorf("failed to remove publish directory %s: %v", targetPath, err)
	}

	klog.V(4).Infof("DetachDisk: successfully cleaned up %s", targetPath)
	return nil
}
