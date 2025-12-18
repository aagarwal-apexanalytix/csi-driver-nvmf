/*
Copyright 2021 The Kubernetes Authors.

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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

func waitForPathToExist(devicePath string, maxRetries, intervalSeconds int, deviceTransport string) (bool, error) {
	for i := 0; i < maxRetries; i++ {
		exist := utils.IsFileExisting(devicePath)
		if exist {
			return true, nil
		}
		if i == maxRetries-1 {
			break
		}
		time.Sleep(time.Second * time.Duration(intervalSeconds))
	}
	return false, fmt.Errorf("not found devicePath %s and transport %s", devicePath, deviceTransport)
}

// getNvmeInfoByNqn discovers the controller and raw device path for a given subsystem NQN
func getNvmeInfoByNqn(nqn string) (controller string, devicePath string, err error) {
	const ctlPath = "/sys/class/nvme-fabrics/ctl"

	entries, err := os.ReadDir(ctlPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to read %s: %w", ctlPath, err)
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "nvme") {
			continue
		}

		subsysPath := filepath.Join(ctlPath, entry.Name(), "subsysnqn")
		data, err := os.ReadFile(subsysPath)
		if err != nil {
			continue
		}

		if strings.TrimSpace(string(data)) == nqn {
			controller = entry.Name()

			// Scan for namespace directories (e.g., nvme0c1n2)
			nsEntries, err := os.ReadDir(filepath.Join(ctlPath, entry.Name()))
			if err != nil {
				return controller, "", err
			}

			for _, ns := range nsEntries {
				nsName := ns.Name()
				if strings.HasPrefix(nsName, "nvme") && strings.Contains(nsName, "n") {
					// Clean namespace name (remove 'c' part, e.g., nvme0c1n2 → nvme0n2)
					cleanNs := strings.Replace(nsName, "c", "", -1)
					return controller, "/dev/" + cleanNs, nil
				}
			}

			return controller, "", fmt.Errorf("no namespace found under controller %s for NQN %s", controller, nqn)
		}
	}

	return "", "", fmt.Errorf("no controller found for NQN %s", nqn)
}

func GetDeviceNameByVolumeID(volumeID string) (string, error) {
	_, devicePath, err := getNvmeInfoByNqn(volumeID)
	if err != nil {
		return "", fmt.Errorf("failed to find device for volumeID %s: %w", volumeID, err)
	}
	return devicePath, nil
}

// parseDeviceToControllerPath returns the correct rescan path for NVMe-oF fabrics controllers
func parseDeviceToControllerPath(volumeID string) (string, error) {
	controller, _, err := getNvmeInfoByNqn(volumeID)
	if err != nil {
		return "", err
	}
	return filepath.Join("/sys/class/nvme-fabrics/ctl", controller, "rescan_controller"), nil
}

func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.Infof("GRPC call: %s", info.FullMethod)
	klog.Infof("GRPC request: %s", protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("GRPC error: %v", err)
	} else {
		klog.Infof("GRPC response: %s", protosanitizer.StripSecrets(resp))
	}
	return resp, err
}

func Rollback(err error, fc func()) {
	if err != nil {
		if fc != nil {
			klog.Infof("Executing rollback func:%s for error: %v", runtime.FuncForPC(reflect.ValueOf(fc).Pointer()).Name(), err)
		}
		fc()
	}
}
