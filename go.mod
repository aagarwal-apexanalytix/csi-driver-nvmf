module github.com/kubernetes-csi/csi-driver-nvmf

go 1.25.5

require (
	github.com/container-storage-interface/spec v1.12.0
	github.com/csi-addons/spec v0.2.1-0.20250610152019-b5a7205f6a79
	github.com/kubernetes-csi/csi-lib-utils v0.23.0
	golang.org/x/net v0.48.0
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.11
	k8s.io/klog/v2 v2.130.1
	k8s.io/utils v0.0.0-20251222233032-718f0e51e6d2
)

require (
	github.com/go-logr/logr v1.4.3 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b // indirect
)
