package aws_test

import (
	"testing"

	"github.com/stackshy/cloudemu/v2/config"
	"github.com/stackshy/cloudemu/v2/providers/aws"
)

// TestTickablesRegistersTimeDrivenServices checks the serve ticker reaches the
// region's own SQS, EFS, CloudWatch, SSM, CloudFormation and Batch, so queue
// gauges, EFS metrics, due alarms, parameter policies, settling stack operations
// and Batch job state-change events run without a read. SQS and EFS come before
// CloudWatch so alarms see the metrics published in the same tick.
func TestTickablesRegistersTimeDrivenServices(t *testing.T) {
	p := aws.New()

	got := p.Tickables()
	if len(got) != 6 || got[0] != config.Tickable(p.SQS) || got[1] != config.Tickable(p.EFS) ||
		got[2] != config.Tickable(p.CloudWatch) || got[3] != config.Tickable(p.SSM) ||
		got[4] != config.Tickable(p.CloudFormation) || got[5] != config.Tickable(p.Batch) {
		t.Fatalf("Tickables() = %v, want this provider's SQS, EFS, CloudWatch, SSM, CloudFormation and Batch", got)
	}
}
