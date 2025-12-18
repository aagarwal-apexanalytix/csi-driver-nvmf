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

func GetDeviceNameByVolumeID(volumeID string) (string, error) {
	// Use NQN-based discovery (volumeID == NQN in your setup)
	devicePath, err := getDevicePathByNqn(volumeID)
	if err != nil {
		return "", fmt.Errorf("failed to find device for volumeID %s: %w", volumeID, err)
	}
	return devicePath, nil
}

func parseDeviceToControllerPath(deviceName string) string {
	nvmfControllerPrefix := "/sys/class/block"
	index := strings.LastIndex(deviceName, "n")
	parsed := deviceName[:index] + "c0" + deviceName[index:]
	scanPath := filepath.Join(nvmfControllerPrefix, parsed, "device/rescan_controller")
	return scanPath
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
