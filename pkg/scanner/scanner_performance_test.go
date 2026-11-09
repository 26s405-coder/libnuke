package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ekristen/libnuke/pkg/registry"
)

func TestRunClosesItemsAfterCanceledListerFinishes(t *testing.T) {
	registry.ClearRegistry()
	registry.Register(testResourceRegistration)

	s, err := New(&Config{
		Owner:           "owner",
		ResourceTypes:   []string{testResourceType},
		Opts:            TestOpts{Sleep: 100 * time.Millisecond},
		ParallelQueries: 1,
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = s.Run(ctx)
	assert.Error(t, err)

	itemCount := make(chan int, 1)
	go func() {
		count := 0
		for range s.Items {
			count++
		}
		itemCount <- count
	}()

	select {
	case count := <-itemCount:
		assert.Equal(t, 1, count)
	case <-time.After(time.Second):
		t.Fatal("scanner item channel was not closed after the lister finished")
	}
}

func TestRunReturnsErrorWhenContextIsAlreadyCanceled(t *testing.T) {
	registry.ClearRegistry()
	registry.Register(testResourceRegistration)

	s, err := New(&Config{
		Owner:           "owner",
		ResourceTypes:   []string{testResourceType},
		ParallelQueries: 1,
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}
