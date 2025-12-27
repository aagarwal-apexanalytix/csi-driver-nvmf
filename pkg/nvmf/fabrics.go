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
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kubernetes-csi/csi-driver-nvmf/pkg/utils"
	"k8s.io/klog/v2"
)

type Connector struct {
	VolumeID      string
	DeviceID      string
	TargetNqn     string
	TargetAddr    string
	TargetPort    string
	Transport     string
	HostNqn       string
	HostId        string
	RetryCount    int32
	CheckInterval int32
}

func getNvmfConnector(nvmfInfo *nvmfDiskInfo) *Connector {
	hostnqn := ""
	if nvmfInfo.HostNqn != "" {
		hostnqn = nvmfInfo.HostNqn
	} else {
		if hostnqnData, err := os.ReadFile("/etc/nvme/hostnqn"); err == nil {
			hostnqn = strings.TrimSpace(string(hostnqnData))
		}
	}

	hostid := ""
	if nvmfInfo.HostId != "" {
		hostid = nvmfInfo.HostId
	} else {
		if hostidData, err := os.ReadFile("/etc/nvme/hostid"); err == nil {
			hostid = strings.TrimSpace(string(hostidData))
		}
	}

	return &Connector{
		VolumeID:   nvmfInfo.VolName,
		DeviceID:   nvmfInfo.DeviceID,
		TargetNqn:  nvmfInfo.Nqn,
		TargetAddr: nvmfInfo.Addr,
		TargetPort: nvmfInfo.Port,
		Transport:  nvmfInfo.Transport,
		HostNqn:    hostnqn,
		HostId:     hostid,
	}
}

// connector provides a struct to hold all of the needed parameters to make nvmf connection

func _connect(argStr string) error {
	file, err := os.OpenFile("/dev/nvme-fabrics", os.O_RDWR, 0666)
	if err != nil {
		klog.Errorf("Connect: open NVMf fabrics error: %v", err)
		return err
	}
	defer file.Close()

	if err := utils.WriteStringToFile(file, argStr); err != nil {
		klog.Errorf("Connect: write arg to connect file error: %v", err)
		return err
	}

	// Best-effort verification read
	if lines, rerr := utils.ReadLinesFromFile(file); rerr == nil {
		klog.V(5).Infof("Connect: read back %v", lines)
	}
	return nil
}

func _disconnect(sysfsPath string) error {
	file, err := os.OpenFile(sysfsPath, os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := utils.WriteStringToFile(file, "1"); err != nil {
		klog.Errorf("Disconnect: write 1 to delete_controller error: %v", err)
		return err
	}
	return nil
}

func disconnectSubsysWithHostNqn(nqn, hostnqn, ctrl string) error {
	sysfsSubsysnqnPath := fmt.Sprintf("%s/%s/subsysnqn", SYS_NVMF, ctrl)
	sysfsHostnqnPath := fmt.Sprintf("%s/%s/hostnqn", SYS_NVMF, ctrl)
	sysfsDelPath := fmt.Sprintf("%s/%s/delete_controller", SYS_NVMF, ctrl)

	file, err := os.Open(sysfsSubsysnqnPath)
	if err != nil {
		return &NoControllerError{Nqn: nqn, Hostnqn: hostnqn}
	}
	defer file.Close()

	lines, err := utils.ReadLinesFromFile(file)
	if err != nil || len(lines) == 0 {
		return &NoControllerError{Nqn: nqn, Hostnqn: hostnqn}
	}
	if lines[0] != nqn {
		return &NoControllerError{Nqn: nqn, Hostnqn: hostnqn}
	}

	file, err = os.Open(sysfsHostnqnPath)
	if err != nil {
		return &UnsupportedHostnqnError{Target: sysfsHostnqnPath}
	}
	defer file.Close()

	lines, err = utils.ReadLinesFromFile(file)
	if err != nil || len(lines) == 0 {
		return &NoControllerError{Nqn: nqn, Hostnqn: hostnqn}
	}
	if lines[0] != hostnqn {
		return &NoControllerError{Nqn: nqn, Hostnqn: hostnqn}
	}

	if err := _disconnect(sysfsDelPath); err != nil {
		return err
	}
	return nil
}

func disconnectSubsys(nqn, ctrl string) error {
	sysfsSubsysnqnPath := fmt.Sprintf("%s/%s/subsysnqn", SYS_NVMF, ctrl)
	sysfsDelPath := fmt.Sprintf("%s/%s/delete_controller", SYS_NVMF, ctrl)

	file, err := os.Open(sysfsSubsysnqnPath)
	if err != nil {
		return &NoControllerError{Nqn: nqn, Hostnqn: ""}
	}
	defer file.Close()

	lines, err := utils.ReadLinesFromFile(file)
	if err != nil || len(lines) == 0 {
		return &NoControllerError{Nqn: nqn, Hostnqn: ""}
	}
	if lines[0] != nqn {
		return &NoControllerError{Nqn: nqn, Hostnqn: ""}
	}

	if err := _disconnect(sysfsDelPath); err != nil {
		return err
	}
	return nil
}

// disconnectByNqn disconnects any matching controllers for the given subsystem NQN.
// It is designed to be idempotent: if state files are missing, it still attempts sysfs disconnects.
func disconnectByNqn(nqn, hostnqn string) int {
	ret := 0
	if len(nqn) > NVMF_NQN_SIZE {
		klog.Errorf("Disconnect: nqn %s is too long", nqn)
		return -EINVAL
	}

	// Best-effort: remove hostnqn marker file, if present
	if hostnqn != "" {
		hostnqnPath := filepath.Join(RUN_NVMF, nqn, b64.StdEncoding.EncodeToString([]byte(hostnqn)))
		_ = os.Remove(hostnqnPath)
	}

	// Determine whether we still have hostnqn marker files (if RUN_NVMF/nqn exists)
	nqnPath := filepath.Join(RUN_NVMF, nqn)
	var hostnqnFiles []os.DirEntry
	if entries, err := os.ReadDir(nqnPath); err == nil {
		hostnqnFiles = entries
		// If empty after deletion, remove the whole nqn dir (best-effort)
		if len(hostnqnFiles) == 0 {
			_ = os.RemoveAll(nqnPath)
		}
	} else if !os.IsNotExist(err) {
		// Unexpected error reading RUN_NVMF; keep going with sysfs disconnect attempts.
		klog.Warningf("Disconnect: readdir %s err: %v (continuing best-effort)", nqnPath, err)
	}

	// Read current fabrics controllers
	devices, err := os.ReadDir(SYS_NVMF)
	if err != nil {
		klog.Errorf("Disconnect: readdir %s err: %v", SYS_NVMF, err)
		return -ENOENT
	}

	if hostnqn != "" {
		// Disconnect only controllers matching (nqn, hostnqn)
		for _, device := range devices {
			if err := disconnectSubsysWithHostNqn(nqn, hostnqn, device.Name()); err == nil {
				ret++
			}
		}
		return ret
	}

	// If hostnqn is empty, we disconnect only when there are no hostnqn marker files left
	// (i.e., no remaining consumers).
	if len(hostnqnFiles) > 0 {
		klog.V(5).Infof("Disconnect: skipping disconnect for nqn=%s because hostnqn markers still exist (%d)", nqn, len(hostnqnFiles))
		return ret
	}

	for _, device := range devices {
		if err := disconnectSubsys(nqn, device.Name()); err == nil {
			ret++
		}
	}
	return ret
}

// getDevicePathByNqn returns the local device path for a given subsystem NQN.
// Prefer the namespace-aware discovery via getNvmeInfoByNqn() if available in the package.
func getDevicePathByNqn(nqn string) (string, error) {
	// If the package has getNvmeInfoByNqn, it is more accurate because it finds namespace IDs.
	if _, dev, err := getNvmeInfoByNqn(nqn); err == nil && dev != "" {
		return dev, nil
	}

	// Fallback: assume namespace 1.
	const sysNvmeFabricsPath = "/sys/class/nvme-fabrics/ctl"
	entries, err := os.ReadDir(sysNvmeFabricsPath)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", sysNvmeFabricsPath, err)
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "nvme") {
			continue
		}
		subsysPath := filepath.Join(sysNvmeFabricsPath, entry.Name(), "subsysnqn")
		data, err := os.ReadFile(subsysPath)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(data)) == nqn {
			return "/dev/" + entry.Name() + "n1", nil
		}
	}
	return "", fmt.Errorf("no NVMe device found for subsystem NQN %s", nqn)
}

func (c *Connector) Connect() (string, error) {
	if c.RetryCount == 0 {
		c.RetryCount = 10
	}
	if c.CheckInterval == 0 {
		c.CheckInterval = 1
	}
	if c.RetryCount < 0 || c.CheckInterval < 0 {
		return "", fmt.Errorf("invalid RetryCount/CheckInterval: RetryCount=%d, CheckInterval=%d", c.RetryCount, c.CheckInterval)
	}

	transport := strings.ToLower(strings.TrimSpace(c.Transport))
	if transport != "tcp" && transport != "rdma" {
		return "", fmt.Errorf("csi transport only supports tcp/rdma")
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("nqn=%s,transport=%s,traddr=%s,trsvcid=%s", c.TargetNqn, transport, c.TargetAddr, c.TargetPort))
	if c.HostNqn != "" {
		builder.WriteString(fmt.Sprintf(",hostnqn=%s", c.HostNqn))
	}
	if c.HostId != "" {
		builder.WriteString(fmt.Sprintf(",hostid=%s", c.HostId))
	}
	baseString := builder.String()

	// Idempotent: if already connected, ensure RUN_NVMF markers and return.
	if devicePath, err := getDevicePathByNqn(c.TargetNqn); err == nil && devicePath != "" {
		klog.Infof("Volume %s already connected at %s (idempotent connect)", c.VolumeID, devicePath)
		_ = ensureMarkers(c.TargetNqn, c.HostNqn)
		return devicePath, nil
	}

	// Not connected — perform connect
	if err := _connect(baseString); err != nil {
		// Tolerate "already in progress" and retry discovery
		if strings.Contains(strings.ToLower(err.Error()), "operation already in progress") {
			klog.Infof("Connect reported 'already in progress' for %s — retrying discovery", c.VolumeID)
			time.Sleep(2 * time.Second)
		} else {
			return "", err
		}
	}

	// Discover device path with configured retries
	var devicePath string
	var derr error
	for i := int32(0); i < c.RetryCount; i++ {
		devicePath, derr = getDevicePathByNqn(c.TargetNqn)
		if derr == nil && devicePath != "" {
			if _, statErr := os.Stat(devicePath); statErr == nil {
				break
			}
		}
		time.Sleep(time.Duration(c.CheckInterval) * time.Second)
	}

	if devicePath == "" {
		klog.Errorf("Failed to discover device after connect for nqn %s: %v — rollback", c.TargetNqn, derr)
		if r := disconnectByNqn(c.TargetNqn, c.HostNqn); r < 0 {
			klog.Errorf("Rollback disconnect failed")
		}
		if derr != nil {
			return "", derr
		}
		return "", fmt.Errorf("device not found for nqn %s", c.TargetNqn)
	}

	// Persistence markers (best-effort, but treat errors as rollback-worthy because they affect disconnect semantics)
	if err := ensureMarkers(c.TargetNqn, c.HostNqn); err != nil {
		klog.Errorf("Failed to create markers for nqn=%s hostnqn=%s: %v — rollback", c.TargetNqn, c.HostNqn, err)
		if r := disconnectByNqn(c.TargetNqn, c.HostNqn); r < 0 {
			klog.Errorf("Rollback failed")
		}
		return "", err
	}

	klog.Infof("After connect returning devicePath: %s", devicePath)
	return devicePath, nil
}

// ensureMarkers creates RUN_NVMF/<nqn> and optional RUN_NVMF/<nqn>/<b64(hostnqn)> marker file.
func ensureMarkers(nqn, hostnqn string) error {
	nqnPath := filepath.Join(RUN_NVMF, nqn)
	if err := os.MkdirAll(nqnPath, 0750); err != nil {
		return err
	}
	if hostnqn != "" {
		hostnqnPath := filepath.Join(nqnPath, b64.StdEncoding.EncodeToString([]byte(hostnqn)))
		// Create marker file if missing (no truncation required, but Create is fine for a marker).
		if _, err := os.Stat(hostnqnPath); os.IsNotExist(err) {
			f, err := os.OpenFile(hostnqnPath, os.O_CREATE|os.O_EXCL, 0640)
			if err != nil {
				// If another process created it, that's fine.
				if !os.IsExist(err) {
					return err
				}
			} else {
				_ = f.Close()
			}
		}
	}
	return nil
}

// Disconnect we disconnect only by nqn (and optional hostnqn marker)
func (c *Connector) Disconnect() error {
	ret := disconnectByNqn(c.TargetNqn, c.HostNqn)
	if ret < 0 {
		return fmt.Errorf("Disconnect: failed to disconnect by nqn: %s", c.TargetNqn)
	}
	return nil
}

// persistConnectorFile persists the provided Connector to the specified file (e.g. /var/lib/.../myConnector.json).
func persistConnectorFile(c *Connector, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating nvmf persistence file %s: %s", filePath, err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	if err := encoder.Encode(c); err != nil {
		return fmt.Errorf("error encoding connector: %v", err)
	}
	return nil
}

func removeConnectorFile(filePath string) {
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		klog.Errorf("Can't remove connector file %s: %v", filePath, err)
	}
}

func GetConnectorFromFile(filePath string) (*Connector, error) {
	b, err := os.ReadFile(filePath)
	if err != nil {
		return &Connector{}, err
	}
	data := Connector{}
	if err := json.Unmarshal(b, &data); err != nil {
		return &Connector{}, err
	}
	return &data, nil
}
