package csi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/docker/swarmkit/agent/exec"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"
)

// volumes is a map that keeps all the currently available volumes to the agent
// mapped by volume ID.
type volumes struct {
	mu        sync.RWMutex                     // To sync between Add(), Remove() and Reset() for "m" map
	tryMu     sync.RWMutex                     // To sync between tryAddVolume() and tryRemoveVolume()
	mapMu     sync.RWMutex                     // To sync access to "pluginMap" map
	m         map[string]*api.VolumeAssignment // Map between VolumeID and VolumeAssignment
	pluginMap map[string]*NodePlugin           // Map between Driver Name and NodePlugin
}

const maxRetries int = 25

const initialBackoff = 100 * time.Millisecond

// NewManager returns a place to store volumes.
func NewManager() exec.VolumesManager {
	return &volumes{
		m:         make(map[string]*api.VolumeAssignment),
		pluginMap: make(map[string]*NodePlugin),
	}
}

// Get returns a volume published path for the provided volume ID.  If the volume doesn't exist, returns empty string.
func (r *volumes) Get(volumeID string) (string, error) {
	r.mapMu.RLock()
	defer r.mapMu.RUnlock()
	if plugin, ok := r.pluginMap[volumeID]; ok {
		return plugin.GetPublisedPath(volumeID)
	}
	return "", nil
}

// Add adds one or more volumes to the volume map.
func (r *volumes) Add(volumes ...api.VolumeAssignment) {
	r.mu.Lock()
	var volumeObjects []*api.VolumeAssignment
	defer r.mu.Unlock()
	ctx := context.Background()
	for _, volume := range volumes {
		v := volume.Copy()
		log.G(ctx).WithField("method", "(*volumes).Add").Debugf("Add Volume:%v", volume.VolumeID)

		r.m[volume.VolumeID] = v
		driverName := v.Driver.Name
		r.mapMu.Lock()
		if _, ok := r.pluginMap[driverName]; !ok {
			r.pluginMap[driverName] = NewNodePlugin(driverName)
		}
		r.mapMu.Unlock()
		volumeObjects = append(volumeObjects, v)
	}
	go r.iterateVolumes(volumeObjects, true)
}

func (r *volumes) addVolumes(volumeObjects []*api.VolumeAssignment) {
	ctx := context.Background()
	for _, v := range volumeObjects {
		go r.tryAddVolume(ctx, v)
	}
}

func (r *volumes) iterateVolumes(volumeObjects []*api.VolumeAssignment, isAdd bool) {
	ctx := context.Background()
	for _, v := range volumeObjects {
		if isAdd {
			go r.tryAddVolume(ctx, v)
		} else {
			go r.tryRemoveVolume(ctx, v)
		}
	}
}

func (r *volumes) tryAddVolume(ctx context.Context, assignment *api.VolumeAssignment) {
	r.tryMu.Lock()
	defer r.tryMu.Unlock()

	r.mapMu.RLock()
	plugin, ok := r.pluginMap[assignment.VolumeID]
	r.mapMu.RUnlock()
	if !ok {
		log.G(ctx).Debugf("plugin not found for VolumeID:%v", assignment.VolumeID)
		return
	}
	if assignment.StageUnstageVolume {
		if err := plugin.NodeStageVolume(ctx, assignment); err != nil {
			waitFor := initialBackoff
		retryStage:
			for i := 0; i < maxRetries; i++ {
				select {
				case <-ctx.Done():
					// selecting on ctx.Done() allows us to bail out of retrying early
					return
				case <-time.After(waitFor):
					// time.After is better than using time.Sleep, because it blocks
					// on a channel read, rather than suspending the whole
					// goroutine. That lets us do the above check on ctx.Done().
					//
					// time.After is convenient, but it has a key problem: the timer
					// is not garbage collected until the channel fires. this
					// shouldn't be a problem, unless the context is canceled, there
					// is a very long timer, and there are a lot of other goroutines
					// in the same situation.
					if err := plugin.NodeStageVolume(ctx, assignment); err == nil {
						break retryStage
					}
				}
				// if the exponential factor is 2, you can avoid using floats by
				// doing bit shifts. each shift left increases the number by a power
				// of 2. we can do this because Duration is ultimately int64.
				waitFor = waitFor << 1
			}
		}
	}

	// Publish
	if err := plugin.NodePublishVolume(ctx, assignment); err != nil {
		waitFor := initialBackoff
	retryPublish:
		for i := 0; i < maxRetries; i++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(waitFor):
				if err := plugin.NodePublishVolume(ctx, assignment); err == nil {
					break retryPublish
				}
			}
			waitFor = waitFor << 1
		}
	}
}

func (r *volumes) tryRemoveVolume(ctx context.Context, assignment *api.VolumeAssignment) {
	r.tryMu.Lock()
	defer r.tryMu.Unlock()

	r.mapMu.RLock()
	plugin, ok := r.pluginMap[assignment.VolumeID]
	r.mapMu.RUnlock()
	if !ok {
		log.G(ctx).Debugf("plugin not found for VolumeID:%v", assignment.VolumeID)
		return
	}
	var err error
	if err = plugin.NodeUnpublishVolume(ctx, assignment); err != nil {
		waitFor := initialBackoff
	retryUnPublish:
		for i := 0; i < maxRetries; i++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(waitFor):
				if err := plugin.NodeUnpublishVolume(ctx, assignment); err == nil {
					break retryUnPublish
				}
			}
			waitFor = waitFor << 1
		}
	}

	// Unstage
	if assignment.StageUnstageVolume {
		if err = plugin.NodeUnstageVolume(ctx, assignment); err != nil {
			waitFor := initialBackoff
		retryUnstage:
			for i := 0; i < maxRetries; i++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(waitFor):
					if err := plugin.NodeUnstageVolume(ctx, assignment); err == nil {
						break retryUnstage
					}
				}
				waitFor = waitFor << 1
			}
		}
	}
}

// Remove removes one or more volumes by ID from the volumes map. Succeeds
// whether or not the given IDs are in the map.
func (r *volumes) Remove(volumes []string) {
	r.mu.Lock()
	var volumeObjects []*api.VolumeAssignment
	defer r.mu.Unlock()
	ctx := context.Background()
	for _, volume := range volumes {
		v := r.m[volume]
		log.G(ctx).WithField("method", "(*volumes).Remove").Debugf("Remove Volume:%v", volume)
		if v != nil {
			volumeObjects = append(volumeObjects, v)
			name := v.Driver.Name
			r.mapMu.Lock()
			delete(r.pluginMap, name)
			r.mapMu.Unlock()
		}
		delete(r.m, volume)

	}
	go r.iterateVolumes(volumeObjects, false)
}

// Reset removes all the volumes.
func (r *volumes) Reset() {
	r.mu.Lock()
	var volumeObjects []*api.VolumeAssignment
	defer r.mu.Unlock()
	for _, v := range r.m {
		volumeObjects = append(volumeObjects, v)
	}
	r.m = make(map[string]*api.VolumeAssignment)
	r.mapMu.Lock()
	r.pluginMap = make(map[string]*NodePlugin)
	r.mapMu.Unlock()

	go r.iterateVolumes(volumeObjects, false)
}

func (r *volumes) removeVolumes(volumeObjects []*api.VolumeAssignment) {
	ctx := context.Background()
	for _, v := range volumeObjects {
		go r.tryRemoveVolume(ctx, v)
	}
}

// taskRestrictedVolumesProvider restricts the ids to the task.
type taskRestrictedVolumesProvider struct {
	volumes   exec.VolumeGetter
	volumeIDs map[string]struct{}
}

func (sp *taskRestrictedVolumesProvider) Get(volumeID string) (string, error) {
	if _, ok := sp.volumeIDs[volumeID]; !ok {
		return "", fmt.Errorf("task not authorized to access volume %s", volumeID)
	}

	return sp.volumes.Get(volumeID)
}

// Restrict provides a getter that only allows access to the volumes
// referenced by the task.
func Restrict(volumes exec.VolumeGetter, t *api.Task) exec.VolumeGetter {
	vids := map[string]struct{}{}

	for _, v := range t.Volumes {
		vids[v.ID] = struct{}{}
	}

	return &taskRestrictedVolumesProvider{volumes: volumes, volumeIDs: vids}
}
