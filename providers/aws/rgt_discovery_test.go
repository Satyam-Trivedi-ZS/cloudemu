package aws

import (
	"context"
	"errors"
	"sync"
	"testing"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
	ksdriver "github.com/stackshy/cloudemu/v2/services/keyspaces/driver"
	"github.com/stackshy/cloudemu/v2/services/resourcediscovery"
)

// A resource deleted between the list call and its tag read makes the tag read
// return NotFound. project must drop that resource instead of failing, since
// the error would otherwise abort DiscoverResources for every service. Any
// other error must still propagate.
func TestProjectSkipsVanishedResource(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	read := func(_ context.Context, name string) (resourcediscovery.DiscoveredResource, error) {
		switch name {
		case "gone":
			return resourcediscovery.DiscoveredResource{}, cerrors.Newf(cerrors.NotFound, "resource %q not found", name)
		case "broken":
			return resourcediscovery.DiscoveredResource{}, boom
		default:
			return row("svc", "thing", name, "arn:"+name, nil), nil
		}
	}

	rows, err := project(ctx, []string{"a", "gone", "b"}, read)
	if err != nil {
		t.Fatalf("project with a vanished resource: %v", err)
	}

	if len(rows) != 2 || rows[0].ID != "a" || rows[1].ID != "b" {
		t.Fatalf("rows = %+v, want a and b only", rows)
	}

	if _, err := project(ctx, []string{"a", "broken"}, read); !errors.Is(err, boom) {
		t.Fatalf("project with a failing read: err = %v, want boom", err)
	}
}

// A DeleteKeyspace racing the Keyspaces discovery walk must not fail it.
// ListTagsForResource returns NotFound for a keyspace that is gone, so without
// the skip in project nearly every walk that overlaps the delete loop fails.
func TestKeyspacesDiscoverySurvivesConcurrentDelete(t *testing.T) {
	ctx := context.Background()
	p := New()
	d := taggedDiscovery{p}

	if _, err := p.Keyspaces.CreateKeyspace(ctx, ksdriver.CreateKeyspaceConfig{
		Name: "stable", Tags: map[string]string{"env": "prod"},
	}); err != nil {
		t.Fatalf("CreateKeyspace stable: %v", err)
	}

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			_, _ = p.Keyspaces.CreateKeyspace(ctx, ksdriver.CreateKeyspaceConfig{Name: "churn"})
			_ = p.Keyspaces.DeleteKeyspace(ctx, "churn")
		}
	}()

	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := 0; i < 2000; i++ {
		rows, err := d.discoverKeyspaces(ctx)
		if err != nil {
			t.Fatalf("walk %d: discoverKeyspaces: %v", i, err)
		}

		var found bool

		for _, r := range rows {
			if r.ID == "stable" && r.Tags["env"] == "prod" {
				found = true
			}
		}

		if !found {
			t.Fatalf("walk %d: stable keyspace missing or untagged: %+v", i, rows)
		}
	}
}
