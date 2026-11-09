package nuke

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ekristen/libnuke/pkg/queue"
	"github.com/ekristen/libnuke/pkg/registry"
	"github.com/ekristen/libnuke/pkg/resource"
)

type indexedWaitTestResource struct {
	id         string
	filterFail bool
}

func (r *indexedWaitTestResource) Remove(context.Context) error { return nil }
func (r *indexedWaitTestResource) UniqueKey() string             { return r.id }
func (r *indexedWaitTestResource) Filter() error {
	if r.filterFail {
		return fmt.Errorf("resource is no longer eligible")
	}
	return nil
}

type legacyWaitTestResource struct{}

func (r *legacyWaitTestResource) Remove(context.Context) error { return nil }

func TestHandleWaitUsesUniqueKeyIndex(t *testing.T) {
	tests := []struct {
		name          string
		itemKey       string
		listed        []resource.Resource
		expectedState queue.ItemState
	}{
		{
			name:    "matching resource remains waiting",
			itemKey: "resource-1",
			listed: []resource.Resource{
				&indexedWaitTestResource{id: "resource-1"},
			},
			expectedState: queue.ItemStateWaiting,
		},
		{
			name:    "missing resource is finished",
			itemKey: "resource-missing",
			listed: []resource.Resource{
				&indexedWaitTestResource{id: "resource-1"},
			},
			expectedState: queue.ItemStateFinished,
		},
		{
			name:    "filter failure finishes matching resource",
			itemKey: "resource-1",
			listed: []resource.Resource{
				&indexedWaitTestResource{id: "resource-1", filterFail: true},
			},
			expectedState: queue.ItemStateFinished,
		},
		{
			name:    "mixed legacy results use equality fallback",
			itemKey: "resource-1",
			listed: []resource.Resource{
				&legacyWaitTestResource{},
				&indexedWaitTestResource{id: "resource-1"},
			},
			expectedState: queue.ItemStateWaiting,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := New(testParameters, nil, nil)
			item := &queue.Item{
				Resource: &indexedWaitTestResource{id: tc.itemKey},
				State:    queue.ItemStateWaiting,
				Type:     "IndexedWaitTestResource",
				Owner:    "test-account",
			}
			cache := ListCache{
				"test-account": {
					"IndexedWaitTestResource": tc.listed,
				},
			}

			n.handleWait(context.Background(), item, cache, make(waitCacheIndex))

			assert.Equal(t, tc.expectedState, item.State)
		})
	}
}

func TestQueueStateCountsTracksTransitions(t *testing.T) {
	counts := newQueueStateCounts([]*queue.Item{
		{Type: "ResourceA", State: queue.ItemStateNew},
		{Type: "ResourceA", State: queue.ItemStateFinished},
		{Type: "ResourceB", State: queue.ItemStatePending},
	})

	counts.move("ResourceA", queue.ItemStateNew, queue.ItemStatePending)

	assert.Equal(t, 0, counts.countByType("ResourceA", queue.ItemStateNew))
	assert.Equal(t, 2, counts.countByType("ResourceA", queue.ItemStatePending, queue.ItemStateFinished))
	assert.Equal(t, 1, counts.countByType("ResourceB", queue.ItemStatePending))
}

type concurrentScanTestLister struct {
	id      string
	started chan<- struct{}
	release <-chan struct{}
}

func (l concurrentScanTestLister) List(ctx context.Context, _ interface{}) ([]resource.Resource, error) {
	l.started <- struct{}{}
	select {
	case <-l.release:
		return []resource.Resource{&TestResourceSuccess{id: l.id}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestScanRunsRegisteredScannersConcurrently(t *testing.T) {
	registry.ClearRegistry()

	started := make(chan struct{}, 2)
	release := make(chan struct{})

	registry.Register(&registry.Registration{
		Name:   "ConcurrentScanA",
		Scope:  "account",
		Lister: concurrentScanTestLister{id: "a", started: started, release: release},
	})
	registry.Register(&registry.Registration{
		Name:   "ConcurrentScanB",
		Scope:  "account",
		Lister: concurrentScanTestLister{id: "b", started: started, release: release},
	})

	n := New(testParameters, nil, nil)
	first, err := scanner.New(&scanner.Config{
		Owner:         "account-a",
		ResourceTypes: []string{"ConcurrentScanA"},
	})
	assert.NoError(t, err)
	second, err := scanner.New(&scanner.Config{
		Owner:         "account-b",
		ResourceTypes: []string{"ConcurrentScanB"},
	})
	assert.NoError(t, err)
	assert.NoError(t, n.RegisterScanner(testScope, first))
	assert.NoError(t, n.RegisterScanner(testScope, second))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	scanDone := make(chan error, 1)
	go func() {
		scanDone <- n.Scan(ctx)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			cancel()
			<-scanDone
			t.Fatal("registered scanners did not start concurrently")
		}
	}

	close(release)

	select {
	case err := <-scanDone:
		assert.NoError(t, err)
		assert.Equal(t, 2, n.Queue.Total())
	case <-time.After(time.Second):
		t.Fatal("concurrent scan did not complete")
	}
}
