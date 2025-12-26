// Package nvmf controller_types.go
package nvmf

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
)

const (
	ProviderMikroTik  = "mikrotik"
	ProviderNvmeProxy = "nvme-proxy"
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
	provider     string // driver-wide ("mikrotik" | "nvme-proxy" | "static")
}

func NewControllerServer(d *driver) *ControllerServer {
	cs := &ControllerServer{
		Driver:   d,
		client:   &http.Client{Timeout: 30 * time.Second},
		restURL:  os.Getenv("BACKEND_REST_URL"),
		username: os.Getenv("BACKEND_USERNAME"),
		password: os.Getenv("BACKEND_PASSWORD"),
		provider: strings.ToLower(strings.TrimSpace(os.Getenv("CSI_PROVIDER"))),
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
		// For MikroTik legacy: mount name under /disk/btrfs; for nvme-proxy this is a backend selector (btrfs|zfs) in many setups.
		cs.backendMount = "btrfs"
	}
	if cs.provider == "" {
		cs.provider = ProviderStatic
	}
	if cs.provider != ProviderMikroTik && cs.provider != ProviderNvmeProxy && cs.provider != ProviderStatic {
		klog.Warningf("Unknown CSI_PROVIDER=%q; defaulting to %q", cs.provider, ProviderStatic)
		cs.provider = ProviderStatic
	}
}
