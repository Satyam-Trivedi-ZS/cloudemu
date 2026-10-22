package rds

import (
	"context"
	"sync"
	"testing"

	"github.com/stackshy/cloudemu/v2/config"
	cerrors "github.com/stackshy/cloudemu/v2/errors"
	rdsdriver "github.com/stackshy/cloudemu/v2/services/relationaldb/driver"
)

// TestModifyInstanceEngineUpgradeMovesDefaultParamGroup: a major version
// upgrade moves an instance on its default group to the new family's default
// group; a minor upgrade, a deferred upgrade, a custom group and an explicit
// group on the same request are left as the caller expects.
func TestModifyInstanceEngineUpgradeMovesDefaultParamGroup(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()

	create := func(id, group string) {
		t.Helper()

		_, err := m.CreateInstance(ctx, rdsdriver.InstanceConfig{
			ID: id, Engine: "postgres", EngineVersion: "15.4", DBParameterGroupName: group,
		})
		requireNoError(t, err)
	}

	modify := func(id string, in rdsdriver.ModifyInstanceInput) *rdsdriver.Instance {
		t.Helper()

		out, err := m.ModifyInstance(ctx, id, in)
		requireNoError(t, err)

		return out
	}

	create("major", "")
	got := modify("major", rdsdriver.ModifyInstanceInput{EngineVersion: "16.2", ApplyImmediately: true})
	assertEqual(t, "16.2", got.EngineVersion)
	assertEqual(t, "default.postgres16", got.DBParameterGroupName)

	described, err := m.DescribeInstances(ctx, []string{"major"})
	requireNoError(t, err)
	assertEqual(t, "default.postgres16", described[0].DBParameterGroupName)

	create("minor", "")
	got = modify("minor", rdsdriver.ModifyInstanceInput{EngineVersion: "15.5", ApplyImmediately: true})
	assertEqual(t, "default.postgres15", got.DBParameterGroupName)

	create("deferred", "")
	got = modify("deferred", rdsdriver.ModifyInstanceInput{EngineVersion: "16.2"})
	assertEqual(t, "15.4", got.EngineVersion)
	assertEqual(t, "default.postgres15", got.DBParameterGroupName)

	_, err = m.CreateDBParameterGroup(ctx, rdsdriver.ParameterGroupConfig{Name: "custom", Family: "postgres15"})
	requireNoError(t, err)
	create("custom", "custom")
	got = modify("custom", rdsdriver.ModifyInstanceInput{EngineVersion: "16.2", ApplyImmediately: true})
	assertEqual(t, "custom", got.DBParameterGroupName)

	create("explicit", "")
	got = modify("explicit", rdsdriver.ModifyInstanceInput{
		EngineVersion: "16.2", ApplyImmediately: true, DBParameterGroupName: "custom",
	})
	assertEqual(t, "custom", got.DBParameterGroupName)
}

// TestModifyClusterEngineUpgradeMovesDefaultClusterParamGroup mirrors the
// instance case for DBClusterParameterGroupName.
func TestModifyClusterEngineUpgradeMovesDefaultClusterParamGroup(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()

	_, err := m.CreateCluster(ctx, rdsdriver.ClusterConfig{
		ID: "c1", Engine: "aurora-postgresql", EngineVersion: "15.4",
	})
	requireNoError(t, err)

	got, err := m.ModifyCluster(ctx, "c1", rdsdriver.ModifyInstanceInput{EngineVersion: "16.2", ApplyImmediately: true})
	requireNoError(t, err)
	assertEqual(t, "default.aurora-postgresql16", got.DBClusterParameterGroupName)

	got, err = m.ModifyCluster(ctx, "c1", rdsdriver.ModifyInstanceInput{
		EngineVersion: "16.3", DBClusterParameterGroupName: "mine",
	})
	requireNoError(t, err)
	assertEqual(t, "mine", got.DBClusterParameterGroupName)
}

// TestCreateClusterResolvesDefaultEngineVersion: a cluster created without an
// EngineVersion reports the resolved default version, consistent with its
// version-derived default cluster parameter group.
func TestCreateClusterResolvesDefaultEngineVersion(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()

	got, err := m.CreateCluster(ctx, rdsdriver.ClusterConfig{ID: "c1", Engine: "aurora-mysql"})
	requireNoError(t, err)

	want := defaultEngineVersion("aurora-mysql")
	if want == "" {
		t.Fatal("no catalog default version for aurora-mysql")
	}

	assertEqual(t, want, got.EngineVersion)
	assertEqual(t, "default."+parameterGroupFamily("aurora-mysql", want), got.DBClusterParameterGroupName)

	described, err := m.DescribeClusters(ctx, []string{"c1"})
	requireNoError(t, err)
	assertEqual(t, want, described[0].EngineVersion)
}

// TestModifyDefaultParameterGroupRejected: a listed default group cannot be
// modified or reset, and says so (InvalidArgument), instead of NotFound.
func TestModifyDefaultParameterGroupRejected(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()
	params := []rdsdriver.Parameter{{Name: "max_connections", Value: "10"}}

	_, err := m.ModifyDBParameterGroup(ctx, "default.postgres15", params)
	requireCode(t, err, cerrors.IsInvalidArgument)

	_, err = m.ResetDBParameterGroup(ctx, "default.postgres15", nil, true)
	requireCode(t, err, cerrors.IsInvalidArgument)

	_, err = m.ModifyDBClusterParameterGroup(ctx, "default.aurora-postgresql15", params)
	requireCode(t, err, cerrors.IsInvalidArgument)

	_, err = m.ResetDBClusterParameterGroup(ctx, "default.aurora-postgresql15", nil, true)
	requireCode(t, err, cerrors.IsInvalidArgument)

	_, err = m.ModifyDBParameterGroup(ctx, "nope", params)
	requireCode(t, err, cerrors.IsNotFound)

	_, err = m.ModifyDBClusterParameterGroup(ctx, "nope", params)
	requireCode(t, err, cerrors.IsNotFound)
}

// TestTagsOnEventSubscriptionAndDefaultGroup: the "es:" ARN of an event
// subscription and the ARN of a synthesized default parameter group are
// taggable, as Terraform's aws_db_event_subscription requires.
func TestTagsOnEventSubscriptionAndDefaultGroup(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()

	sub, err := m.CreateEventSubscription(ctx, rdsdriver.EventSubscriptionConfig{
		Name: "tfsub", SnsTopicARN: "arn:aws:sns:us-east-1:123456789012:t", Enabled: true,
		Tags: map[string]string{"env": "dev"},
	})
	requireNoError(t, err)

	tags, err := m.ListTagsForResource(ctx, sub.ARN)
	requireNoError(t, err)
	assertEqual(t, "dev", tags["env"])

	requireNoError(t, m.AddTagsToResource(ctx, sub.ARN, map[string]string{"team": "db"}))
	requireNoError(t, m.RemoveTagsFromResource(ctx, sub.ARN, []string{"env"}))

	tags, err = m.ListTagsForResource(ctx, sub.ARN)
	requireNoError(t, err)
	assertEqual(t, 1, len(tags))
	assertEqual(t, "db", tags["team"])

	_, err = m.DeleteEventSubscription(ctx, "tfsub")
	requireNoError(t, err)

	_, err = m.ListTagsForResource(ctx, sub.ARN)
	requireCode(t, err, cerrors.IsNotFound)

	// A re-created subscription of the same name starts untagged.
	_, err = m.CreateEventSubscription(ctx, rdsdriver.EventSubscriptionConfig{
		Name: "tfsub", SnsTopicARN: "arn:aws:sns:us-east-1:123456789012:t", Enabled: true,
	})
	requireNoError(t, err)

	tags, err = m.ListTagsForResource(ctx, sub.ARN)
	requireNoError(t, err)
	assertEqual(t, 0, len(tags))

	pgARN := parameterGroupARN("us-east-1", "123456789012", "default.postgres15")

	tags, err = m.ListTagsForResource(ctx, pgARN)
	requireNoError(t, err)
	assertEqual(t, 0, len(tags))
}

// TestDescribeEventsSourceRulesInProvider: a library caller gets the same
// SourceType / SourceIdentifier validation the wire returns.
func TestDescribeEventsSourceRulesInProvider(t *testing.T) {
	ctx := context.Background()
	m, _ := newEventTestMock()

	_, err := m.DescribeEvents(ctx, rdsdriver.DescribeEventsInput{SourceIdentifier: "db1"})
	requireCode(t, err, cerrors.IsInvalidArgument)
	assertEqual(t, errSourceIdentifierWithoutType, err.(*cerrors.Error).Message) //nolint:errorlint // cerrors.New returns *Error.

	_, err = m.DescribeEvents(ctx, rdsdriver.DescribeEventsInput{SourceType: "db-bogus"})
	requireCode(t, err, cerrors.IsInvalidArgument)

	_, err = m.DescribeEvents(ctx, rdsdriver.DescribeEventsInput{SourceType: "db-instance", SourceIdentifier: "db1"})
	requireNoError(t, err)
}

// TestRestoreSnapshotWithoutEventsClearsLog: loading a snapshot saved before
// any event was recorded leaves no stale events behind.
func TestRestoreSnapshotWithoutEventsClearsLog(t *testing.T) {
	ctx := context.Background()

	empty, _ := newEventTestMock()

	data, err := empty.Snapshot(ctx, false)
	requireNoError(t, err)

	m, _ := newEventTestMock()
	_, err = m.CreateInstance(ctx, rdsdriver.InstanceConfig{ID: "db1", Engine: "mysql"})
	requireNoError(t, err)

	requireNoError(t, m.Restore(ctx, data))

	events, err := m.DescribeEvents(ctx, rdsdriver.DescribeEventsInput{})
	requireNoError(t, err)
	assertEqual(t, 0, len(events))
}

// TestSnapshotConcurrentWithStateWrites runs Snapshot alongside writers of
// the lock-guarded maps and the event log; under -race it fails if Snapshot
// hands json.Marshal a live map instead of a copy taken under the lock.
func TestSnapshotConcurrentWithStateWrites(t *testing.T) {
	ctx := context.Background()
	m := New(config.NewOptions(config.WithRegion("us-east-1"), config.WithAccountID("123456789012")))

	_, err := m.CreateDBParameterGroup(ctx, rdsdriver.ParameterGroupConfig{
		Name: "pg1", Family: "mysql8.0", Tags: map[string]string{"a": "1"},
	})
	requireNoError(t, err)

	pgARN := parameterGroupARN("us-east-1", "123456789012", "pg1")

	const rounds = 50

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := range rounds {
			_ = m.AddTagsToResource(ctx, pgARN, map[string]string{"k": string(rune('a' + i%26))})
			_, _ = m.ModifyDBParameterGroup(ctx, "pg1", []rdsdriver.Parameter{{Name: "x", Value: "1"}})
		}
	}()

	go func() {
		defer wg.Done()

		for range rounds {
			if _, err := m.Snapshot(ctx, false); err != nil {
				t.Errorf("Snapshot: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

func requireCode(t *testing.T, err error, is func(error) bool) {
	t.Helper()

	if err == nil || !is(err) {
		t.Fatalf("unexpected error %v", err)
	}
}
