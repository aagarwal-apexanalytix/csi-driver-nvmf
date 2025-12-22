module github.com/kubernetes-csi/csi-driver-nvmf

go 1.25

require (
	github.com/container-storage-interface/spec v1.12.0
	github.com/kubernetes-csi/csi-lib-utils v0.23.0
	golang.org/x/net v0.48.0
	google.golang.org/grpc v1.77.0
	google.golang.org/protobuf v1.36.11
	k8s.io/klog/v2 v2.130.1
	k8s.io/utils v0.0.0-20251220205832-9d40a56c1308
)

require (
	github.com/go-logr/logr v1.4.3 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251213004720-97cd9d5aeac2 // indirect
)
