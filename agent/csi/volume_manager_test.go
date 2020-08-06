package csi

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/docker/swarmkit/agent/exec"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/identity"
	"github.com/stretchr/testify/assert"
)

const iterations = 25
const interval = 100 * time.Millisecond

func NewFakeManager() *volumes {
	return &volumes{
		m:         make(map[string]*api.VolumeAssignment),
		pluginMap: make(map[string]*NodePlugin),
	}
}

func TestTaskRestrictedVolumesProvider(t *testing.T) {
	type testCase struct {
		desc          string
		volumeIDs     map[string]struct{}
		volumes       exec.VolumeGetter
		volumeID      string
		taskID        string
		volumeIDToGet string
		value         string
		expected      string
		expectedErr   string
	}

	originalvolumeID := identity.NewID()
	taskID := identity.NewID()
	taskSpecificID := fmt.Sprintf("%s.%s", originalvolumeID, taskID)
	driver := "plugin-1"
	testCases := []testCase{
		// The default case when not using a volumes driver or not returning.
		// Test to check if volume ID is allowed to access
		{
			desc:     "Test getting volume by original ID when restricted by task",
			value:    "value",
			expected: originalvolumeID,
			volumeIDs: map[string]struct{}{
				originalvolumeID: {},
			},
			volumeID:      originalvolumeID,
			volumeIDToGet: originalvolumeID,
			taskID:        taskID,
		},
		// Test to check if volume ID is not allowed to access
		{
			desc:        "Test attempting to get a volume by task specific ID when volume is added with original ID",
			value:       "value",
			expectedErr: fmt.Sprintf("task not authorized to access volume %s", taskSpecificID),
			volumeIDs: map[string]struct{}{
				originalvolumeID: {},
			},
			volumeID:      originalvolumeID,
			volumeIDToGet: taskSpecificID,
			taskID:        taskID,
		},
	}
	volumesManager := NewFakeManager()
	for _, testCase := range testCases {
		t.Logf("volumeID=%s, taskID=%s, taskSpecificID=%s", originalvolumeID, taskID, taskSpecificID)
		v := &api.VolumeAssignment{
			VolumeID: originalvolumeID,
			Driver:   &api.Driver{Name: driver},
		}
		ctx := context.Background()
		volumesManager.pluginMap[originalvolumeID] = NewNodePlugin(driver)
		volumesManager.tryAddVolume(ctx, v)
		volumesGetter := Restrict(volumesManager, &api.Task{
			ID: taskID,
		})
		(volumesGetter.(*taskRestrictedVolumesProvider)).volumeIDs = testCase.volumeIDs

		volume, err := volumesGetter.Get(testCase.volumeIDToGet)
		if testCase.expectedErr != "" {
			assert.Error(t, err, testCase.desc)
			assert.Equal(t, testCase.expectedErr, err.Error(), testCase.desc)
		} else {
			t.Logf("volumeIDs=%v", originalvolumeID)
			expectedPath := fmt.Sprintf(TargetPath, testCase.expected)
			t.Logf("expectedPath=%v", expectedPath)
			assert.NoError(t, err, testCase.desc)
			require.NotNil(t, volume, testCase.desc)
			assert.Equal(t, expectedPath, volume, testCase.desc)
		}
		volumesManager.Reset()
	}
}
