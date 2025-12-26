package nvmf

import (
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func (cs *ControllerServer) getSourceFstype(sourceVolID string, restURL string, username string, password string) (string, error) {
	disks, err := cs.restGet(cs.diskListPath(), restURL, username, password)
	if err != nil {
		return "", err
	}
	for _, d := range disks {
		if s, ok := d["slot"].(string); ok && s == sourceVolID {
			commentStr, _ := d["comment"].(string)
			if commentStr == "" {
				return "", fmt.Errorf("no comment found for source volume %s", sourceVolID)
			}
			// Parse fstype from comment: "... fstype:xxx ..."
			fstypePrefix := " fstype:"
			prefixIdx := strings.LastIndex(commentStr, fstypePrefix)
			if prefixIdx == -1 {
				return "", fmt.Errorf("fstype not found in comment for source volume %s", sourceVolID)
			}
			fstypeStr := commentStr[prefixIdx+len(fstypePrefix):]
			parts := strings.SplitN(fstypeStr, " ", 2)
			fstype := strings.TrimSpace(parts[0])
			if fstype == "" {
				return "", fmt.Errorf("no fstype value found in comment for source volume %s", sourceVolID)
			}
			return fstype, nil
		}
	}
	return "", fmt.Errorf("source volume %s not found", sourceVolID)
}

func (cs *ControllerServer) handleContentSource(contentSource *csi.VolumeContentSource, localRestURL, localUsername, localPassword string, fstype *string) (string, int64, string, error) {
	if contentSource == nil {
		return "", 0, "", nil
	}

	var parentSubvol string
	var sourceCapacity int64
	var cloneInfo string
	var sourceVolID string

	if snapSrc := contentSource.GetSnapshot(); snapSrc != nil {
		snapID := snapSrc.GetSnapshotId()
		if snapID == "" {
			return "", 0, "", status.Error(codes.InvalidArgument, "Snapshot ID missing")
		}
		parentSubvol = snapID
		cloneInfo = fmt.Sprintf(" restored-from-snapshot:%s", snapID)

		// Expect "snap-<name>-<sourceVolID>"
		if !strings.HasPrefix(snapID, "snap-") {
			return "", 0, "", status.Error(codes.InvalidArgument, "Invalid snapshot ID format")
		}
		temp := strings.TrimPrefix(snapID, "snap-")
		lastDash := strings.LastIndex(temp, "-")
		if lastDash == -1 {
			return "", 0, "", status.Error(codes.InvalidArgument, "Invalid snapshot ID format (missing source volume)")
		}
		sourceVolID = temp[lastDash+1:]

		exists, size, err := cs.volumeExists(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			return "", 0, "", status.Errorf(codes.Internal, "Failed to query source volume size: %v", err)
		}
		if !exists {
			return "", 0, "", status.Error(codes.NotFound, "Source volume for snapshot not found")
		}
		sourceCapacity = size

		if srcFs, err := cs.getSourceFstype(sourceVolID, localRestURL, localUsername, localPassword); err == nil && srcFs != "" && srcFs != *fstype {
			*fstype = srcFs
			klog.V(4).Infof("Overrode fstype with source's value for restore: %s", *fstype)
		}

	} else if volSrc := contentSource.GetVolume(); volSrc != nil {
		sourceVolID = volSrc.GetVolumeId()
		if sourceVolID == "" {
			return "", 0, "", status.Error(codes.InvalidArgument, "Source Volume ID missing")
		}
		parentSubvol = "vol-" + sourceVolID
		cloneInfo = fmt.Sprintf(" cloned-from-volume:%s", sourceVolID)

		exists, size, err := cs.volumeExists(sourceVolID, localRestURL, localUsername, localPassword)
		if err != nil {
			return "", 0, "", status.Errorf(codes.Internal, "Failed to query source volume: %v", err)
		}
		if !exists {
			return "", 0, "", status.Error(codes.NotFound, "Source volume not found")
		}
		sourceCapacity = size

		if srcFs, err := cs.getSourceFstype(sourceVolID, localRestURL, localUsername, localPassword); err == nil && srcFs != "" && srcFs != *fstype {
			*fstype = srcFs
			klog.V(4).Infof("Overrode fstype with source's value for clone: %s", *fstype)
		}
	} else {
		return "", 0, "", status.Error(codes.Unimplemented, "Unknown or unsupported volume content source type")
	}

	return parentSubvol, sourceCapacity, cloneInfo, nil
}
