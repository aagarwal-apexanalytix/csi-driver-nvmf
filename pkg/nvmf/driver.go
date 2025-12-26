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
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

type driver struct {
	name             string
	nodeId           string
	version          string
	region           string
	volumeMapDir     string
	idServer         *IdentityServer
	nodeServer       *NodeServer
	controllerServer *ControllerServer
	cap              []*csi.VolumeCapability_AccessMode
	cscap            []*csi.ControllerServiceCapability
}

// NewDriver creates the identity/node/controller servers
func NewDriver(conf *GlobalConfig) *driver {
	if conf == nil {
		klog.Fatal("GlobalConfig must be provided")
	}
	if conf.DriverName == "" {
		klog.Fatal("DriverName must be specified")
	}
	if conf.NodeID == "" {
		klog.Fatal("NodeID must be specified")
	}
	klog.Infof("Initializing CSI driver: %s version: %s nodeID: %s", conf.DriverName, conf.Version, conf.NodeID)

	return &driver{
		name:         conf.DriverName,
		version:      conf.Version,
		nodeId:       conf.NodeID,
		region:       conf.Region,
		volumeMapDir: conf.NVMfVolumeMapDir,
	}
}

func normalizeProvider(p string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	switch p {
	case ProviderStatic, ProviderMikroTik, ProviderNvmeProxy:
		return p
	case "":
		return ProviderStatic
	default:
		klog.Warningf("Unknown CSI_PROVIDER=%q; defaulting to %q", p, ProviderStatic)
		return ProviderStatic
	}
}

func (d *driver) Run(conf *GlobalConfig) {
	if conf.Endpoint == "" {
		klog.Fatal("Endpoint must be specified")
	}

	// Create volume map directory if configured (used for persisting NVMe connection info)
	if d.volumeMapDir != "" {
		if err := os.MkdirAll(d.volumeMapDir, 0755); err != nil {
			klog.Fatalf("Failed to create volume map directory %s: %v", d.volumeMapDir, err)
		}
		klog.V(4).Infof("Volume map directory ensured: %s", d.volumeMapDir)
	}

	d.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
	})

	// Controller capabilities should match what ControllerServer reports + implements.
	if conf.IsControllerServer {
		provider := normalizeProvider(os.Getenv("CSI_PROVIDER"))

		caps := []csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_GET_CAPACITY,
			csi.ControllerServiceCapability_RPC_GET_VOLUME,
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		}

		// Dynamic features: supported in mikrotik/nvme-proxy modes; not in static mode.
		if provider != ProviderStatic {
			caps = append(caps,
				csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
				csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				csi.ControllerServiceCapability_RPC_GET_SNAPSHOT,
			)
		}

		d.AddControllerServiceCapabilities(caps)
	}

	d.idServer = NewIdentityServer(d)
	d.nodeServer = NewNodeServer(d)

	if conf.IsControllerServer {
		d.controllerServer = NewControllerServer(d)
	}

	klog.Infof("Starting CSI driver: name=%s version=%s nodeID=%s endpoint=%s controller=%t volumeMapDir=%s",
		d.name, d.version, d.nodeId, conf.Endpoint, conf.IsControllerServer, d.volumeMapDir)

	s := NewNonBlockingGRPCServer()
	s.Start(conf.Endpoint, d.idServer, d.controllerServer, d.nodeServer)
	s.Wait()
}

func (d *driver) AddVolumeCapabilityAccessModes(caps []csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability_AccessMode {
	var out []*csi.VolumeCapability_AccessMode
	for _, c := range caps {
		klog.Infof("Enabling volume access mode: %v", c.String())
		out = append(out, &csi.VolumeCapability_AccessMode{Mode: c})
	}
	d.cap = out
	return out
}

func (d *driver) AddControllerServiceCapabilities(cl []csi.ControllerServiceCapability_RPC_Type) {
	var csc []*csi.ControllerServiceCapability
	for _, c := range cl {
		klog.Infof("Enabling controller service capability: %v", c.String())
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}
	d.cscap = append(d.cscap, csc...)
}

func (d *driver) ValidateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}
	for _, cap := range d.cscap {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported controller service capability: %s", c.String())
}
