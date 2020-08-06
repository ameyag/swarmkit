package csi

import (
	"context"
	"fmt"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/docker/swarmkit/api"
)

type NodePluginInterface interface {
	NodeGetInfo(ctx context.Context) (*api.NodeCSIInfo, error)
	NodeStageVolume(ctx context.Context, req *api.VolumeAssignment) error
	NodeUnstageVolume(ctx context.Context, req []*api.VolumeAssignment) error
	NodePublishVolume(ctx context.Context, req []*api.VolumeAssignment) error
	NodeUnpublishVolume(ctx context.Context, req []*api.VolumeAssignment) error
}

type volumePublishStatus struct {
	// targetPath is Target path of volume
	targetPath string

	// isPublished keeps track if the volume is published.
	isPublished bool
}

// plugin represents an individual CSI node plugin
type NodePlugin struct {
	// Name is the name of the plugin, which is also the name used as the
	// Driver.Name field
	Name string

	// Node ID is identifier for the node.
	NodeID string

	// Socket is the unix socket to connect to this plugin at.
	Socket string

	// CC is the grpc client connection
	CC *grpc.ClientConn

	// IDClient is the identity service client
	IDClient csi.IdentityClient

	// NodeClient is the node service client
	NodeClient csi.NodeClient

	// VolumeMap is the map from volume ID to Volume. Will place a volume once it is staged,
	// remove it from the map for unstage.
	// TODO: Make this map persistent if the swarm node goes down
	VolumeMap map[string]*volumePublishStatus

	// Lock for VolumeMap
	Lock sync.RWMutex
}

const TargetPath string = "/var/lib/docker/%s"

func NewNodePlugin(name string) *NodePlugin {
	return &NodePlugin{
		Name:      name,
		VolumeMap: make(map[string]*volumePublishStatus),
	}
}

//// Get returns a volume published path for the provided volume ID.  If the volume doesn't exist, returns empty string.
func (np *NodePlugin) GetPublisedPath(volumeID string) (string, error) {
	np.Lock.RLock()
	defer np.Lock.RUnlock()
	if volInfo, ok := np.VolumeMap[volumeID]; ok {
		if volInfo.isPublished {
			return volInfo.targetPath, nil
		}
		return volInfo.targetPath, fmt.Errorf("volume %s is not published", volumeID)
	}
	return "", nil
}

func (np *NodePlugin) NodeGetInfo(ctx context.Context) (*api.NodeCSIInfo, error) {
	np.Lock.RLock()
	defer np.Lock.RUnlock()
	resp := &csi.NodeGetInfoResponse{
		NodeId: np.NodeID,
	}

	return makeNodeInfo(resp), nil
}

func (np *NodePlugin) NodeStageVolume(ctx context.Context, req *api.VolumeAssignment) error {

	volID := req.VolumeID
	stagingTarget := fmt.Sprintf(TargetPath, volID)

	// Check arguments
	if len(volID) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	np.Lock.Lock()
	defer np.Lock.Unlock()

	v := &volumePublishStatus{
		targetPath: stagingTarget,
	}

	np.VolumeMap[volID] = v

	return nil
}

func (np *NodePlugin) NodeUnstageVolume(ctx context.Context, req *api.VolumeAssignment) error {

	volID := req.VolumeID

	// Check arguments
	if len(volID) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	np.Lock.Lock()
	defer np.Lock.Unlock()
	if v, ok := np.VolumeMap[volID]; ok {
		if v.isPublished {
			return status.Errorf(codes.InvalidArgument, "VolumeID %s is not unpublished", volID)
		}
		delete(np.VolumeMap, volID)
		return nil
	}

	return status.Errorf(codes.NotFound, "VolumeID %s is not staged", volID)
}

func (np *NodePlugin) NodePublishVolume(ctx context.Context, req *api.VolumeAssignment) error {

	volID := req.VolumeID

	// Check arguments
	if len(volID) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	np.Lock.Lock()
	defer np.Lock.Unlock()
	if v, ok := np.VolumeMap[volID]; ok {
		if _, err := os.Stat(v.targetPath); os.IsNotExist(err) {
			os.Mkdir(v.targetPath, os.ModeDir)
		}
		v.isPublished = true
		return nil
	}

	return status.Errorf(codes.NotFound, "VolumeID %s is not staged", volID)
}

func (np *NodePlugin) NodeUnpublishVolume(ctx context.Context, req *api.VolumeAssignment) error {

	volID := req.VolumeID

	// Check arguments
	if len(volID) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	np.Lock.Lock()
	defer np.Lock.Unlock()
	if v, ok := np.VolumeMap[volID]; ok {
		err := os.RemoveAll(v.targetPath)
		if err != nil {
			return err
		}
		v.isPublished = false
		return nil
	}

	return status.Errorf(codes.NotFound, "VolumeID %s is not staged", volID)
}
