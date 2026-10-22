package rds_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	smithy "github.com/aws/smithy-go"

	"github.com/stackshy/cloudemu/v2"
	awsserver "github.com/stackshy/cloudemu/v2/server/aws"
)

// TestWireDescribeEventsRejectsMalformedTimes: a raw query client sending a
// timestamp or Duration that does not parse gets InvalidParameterValue, not
// the default window. The SourceIdentifier-without-SourceType rule, now
// enforced by the provider, still surfaces as InvalidParameterCombination.
func TestWireDescribeEventsRejectsMalformedTimes(t *testing.T) {
	ts := httptest.NewServer(awsserver.New(awsserver.Drivers{RDS: cloudemu.NewAWS().RDS}))
	t.Cleanup(ts.Close)

	cases := []struct {
		name     string
		extra    url.Values
		wantCode string
	}{
		{"bad StartTime", url.Values{"StartTime": {"garbage"}}, "InvalidParameterValue"},
		{"bad EndTime", url.Values{"EndTime": {"2025-13-45"}}, "InvalidParameterValue"},
		{"bad Duration", url.Values{"Duration": {"ten"}}, "InvalidParameterValue"},
		{"id without type", url.Values{"SourceIdentifier": {"db1"}}, "InvalidParameterCombination"},
		{"bogus type", url.Values{"SourceType": {"db-bogus"}}, "InvalidParameterValue"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{"Action": {"DescribeEvents"}, "Version": {"2014-10-31"}}
			for k, v := range tc.extra {
				form[k] = v
			}

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL,
				strings.NewReader(form.Encode()))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "<Code>"+tc.wantCode+"</Code>") {
				t.Fatalf("status %d body %s, want 400 %s", resp.StatusCode, body, tc.wantCode)
			}
		})
	}
}

// TestSDKRDSDefaultGroupUpgradeAndImmutability covers, over the wire, a major
// version upgrade moving the instance to the new default group, a modify of a
// listed default group answering InvalidParameterValue (not NotFound), and an
// event subscription's ARN being taggable.
func TestSDKRDSDefaultGroupUpgradeAndImmutability(t *testing.T) {
	client := newSDKClient(t)
	ctx := context.Background()

	if _, err := client.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("up1"), Engine: aws.String("postgres"),
		EngineVersion: aws.String("15.4"), DBInstanceClass: aws.String("db.t3.micro"),
	}); err != nil {
		t.Fatalf("CreateDBInstance: %v", err)
	}

	mod, err := client.ModifyDBInstance(ctx, &awsrds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String("up1"), EngineVersion: aws.String("16.2"),
		AllowMajorVersionUpgrade: aws.Bool(true), ApplyImmediately: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("ModifyDBInstance: %v", err)
	}

	if g := aws.ToString(mod.DBInstance.DBParameterGroups[0].DBParameterGroupName); g != "default.postgres16" {
		t.Fatalf("parameter group after upgrade = %q, want default.postgres16", g)
	}

	_, err = client.ModifyDBParameterGroup(ctx, &awsrds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String("default.postgres16"),
		Parameters: []rdstypes.Parameter{{
			ParameterName: aws.String("max_connections"), ParameterValue: aws.String("10"),
			ApplyMethod: rdstypes.ApplyMethodImmediate,
		}},
	})

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidParameterValue" {
		t.Fatalf("modify default group err = %v, want InvalidParameterValue", err)
	}

	sub, err := client.CreateEventSubscription(ctx, &awsrds.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("tfsub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:t"),
		Tags:             []rdstypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil {
		t.Fatalf("CreateEventSubscription: %v", err)
	}

	tags, err := client.ListTagsForResource(ctx, &awsrds.ListTagsForResourceInput{
		ResourceName: sub.EventSubscription.EventSubscriptionArn,
	})
	if err != nil || len(tags.TagList) != 1 || aws.ToString(tags.TagList[0].Value) != "dev" {
		t.Fatalf("ListTagsForResource(es ARN) = %+v, err %v", tags, err)
	}
}
