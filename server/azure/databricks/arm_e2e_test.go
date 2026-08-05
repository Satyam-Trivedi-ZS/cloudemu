package databricks_test

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/databricks/armdatabricks"
)

// TestSDKDatabricksARMEndToEndJourney exercises the entire Microsoft.Databricks
// ARM surface (#209) as a real user would, in one realistic flow against a
// single emulator, driving every resource type through the real armdatabricks
// SDK clients: a workspace parent, then access connectors, private endpoint
// connections, private link resources, VNet peerings, outbound dependencies,
// and the provider operations list. It also exercises the review fixes inline
// (idempotent delete, location-change rejection, PATCH identity→None).
func TestSDKDatabricksARMEndToEndJourney(t *testing.T) {
	opts, sub := newARMOptions(t)
	ctx := context.Background()

	// A workspace is the parent for the network sub-resources.
	seedWorkspace(t, opts, testRG, testWS)

	// ---- Access connectors (top-level) ----
	acClient, err := armdatabricks.NewAccessConnectorsClient(sub, fakeCred{}, opts)
	if err != nil {
		t.Fatalf("access connectors client: %v", err)
	}

	acPoller, err := acClient.BeginCreateOrUpdate(ctx, testRG, "ac-e2e", armdatabricks.AccessConnector{
		Location: to.Ptr("eastus"),
		Identity: &armdatabricks.ManagedServiceIdentity{
			Type: to.Ptr(armdatabricks.ManagedServiceIdentityTypeSystemAssigned),
		},
		Tags: map[string]*string{"team": to.Ptr("data")},
	}, nil)
	if err != nil {
		t.Fatalf("AC create: %v", err)
	}

	acCreated, err := acPoller.PollUntilDone(ctx, nil)
	if err != nil {
		t.Fatalf("AC create poll: %v", err)
	}

	if acCreated.Identity == nil || acCreated.Identity.PrincipalID == nil || *acCreated.Identity.PrincipalID == "" {
		t.Fatalf("AC system-assigned identity not synthesized: %+v", acCreated.Identity)
	}

	if acCreated.Properties == nil || *acCreated.Properties.ProvisioningState != armdatabricks.ProvisioningStateSucceeded {
		t.Fatalf("AC provisioning state = %+v", acCreated.Properties)
	}

	// A location change on the existing connector is rejected (400).
	if _, err := acClient.BeginCreateOrUpdate(ctx, testRG, "ac-e2e", armdatabricks.AccessConnector{
		Location: to.Ptr("westus"),
	}, nil); err == nil {
		// The 400 may surface at Begin or Poll; if Begin succeeded, poll must fail.
		t.Fatal("AC location change: expected an error, got nil at Begin")
	}

	// List by resource group returns the connector.
	acPager := acClient.NewListByResourceGroupPager(testRG, nil)

	acCount := 0
	for acPager.More() {
		page, perr := acPager.NextPage(ctx)
		if perr != nil {
			t.Fatalf("AC list page: %v", perr)
		}

		acCount += len(page.Value)
	}

	if acCount != 1 {
		t.Fatalf("AC list-by-RG = %d, want 1", acCount)
	}

	// Delete, then delete again — idempotent (no error on missing).
	delAC(t, ctx, acClient)
	delAC(t, ctx, acClient)

	// ---- Private endpoint connections (workspace sub-resource) ----
	pecClient, err := armdatabricks.NewPrivateEndpointConnectionsClient(sub, fakeCred{}, opts)
	if err != nil {
		t.Fatalf("PEC client: %v", err)
	}

	pecPoller, err := pecClient.BeginCreate(ctx, testRG, testWS, "pec-e2e", armdatabricks.PrivateEndpointConnection{
		Properties: &armdatabricks.PrivateEndpointConnectionProperties{
			PrivateLinkServiceConnectionState: &armdatabricks.PrivateLinkServiceConnectionState{
				Status:      to.Ptr(armdatabricks.PrivateLinkServiceConnectionStatusApproved),
				Description: to.Ptr("approved by e2e"),
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("PEC create: %v", err)
	}

	pecCreated, err := pecPoller.PollUntilDone(ctx, nil)
	if err != nil {
		t.Fatalf("PEC create poll: %v", err)
	}

	if pecCreated.Properties == nil ||
		string(*pecCreated.Properties.PrivateLinkServiceConnectionState.Status) != "Approved" {
		t.Fatalf("PEC status = %+v", pecCreated.Properties)
	}

	pecGot, err := pecClient.Get(ctx, testRG, testWS, "pec-e2e", nil)
	if err != nil {
		t.Fatalf("PEC get: %v", err)
	}

	if pecGot.Name == nil || *pecGot.Name != "pec-e2e" {
		t.Fatalf("PEC get name = %v", pecGot.Name)
	}

	// ---- Private link resources (synthesized catalog) ----
	plrClient, err := armdatabricks.NewPrivateLinkResourcesClient(sub, fakeCred{}, opts)
	if err != nil {
		t.Fatalf("PLR client: %v", err)
	}

	plrPager := plrClient.NewListPager(testRG, testWS, nil)

	plrPage, err := plrPager.NextPage(ctx)
	if err != nil {
		t.Fatalf("PLR list: %v", err)
	}

	if len(plrPage.Value) != 2 {
		t.Fatalf("PLR list = %d, want 2 (databricks_ui_api, browser_authentication)", len(plrPage.Value))
	}

	if _, err := plrClient.Get(ctx, testRG, testWS, "databricks_ui_api", nil); err != nil {
		t.Fatalf("PLR get databricks_ui_api: %v", err)
	}

	// ---- VNet peering (workspace sub-resource) ----
	peerClient, err := armdatabricks.NewVNetPeeringClient(sub, fakeCred{}, opts)
	if err != nil {
		t.Fatalf("peering client: %v", err)
	}

	peerPoller, err := peerClient.BeginCreateOrUpdate(ctx, testRG, testWS, "peer-e2e", armdatabricks.VirtualNetworkPeering{
		Properties: &armdatabricks.VirtualNetworkPeeringPropertiesFormat{
			AllowVirtualNetworkAccess: to.Ptr(true),
			RemoteVirtualNetwork: &armdatabricks.VirtualNetworkPeeringPropertiesFormatRemoteVirtualNetwork{
				ID: to.Ptr("/subscriptions/sub-1/resourceGroups/rg-1/providers/Microsoft.Network/virtualNetworks/remote"),
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("peering create: %v", err)
	}

	peerCreated, err := peerPoller.PollUntilDone(ctx, nil)
	if err != nil {
		t.Fatalf("peering create poll: %v", err)
	}

	if peerCreated.Properties == nil ||
		*peerCreated.Properties.PeeringState != armdatabricks.PeeringStateConnected {
		t.Fatalf("peering state = %+v", peerCreated.Properties)
	}

	if !*peerCreated.Properties.AllowVirtualNetworkAccess {
		t.Fatal("peering AllowVirtualNetworkAccess not echoed")
	}

	// ---- Outbound network dependencies ----
	obClient, err := armdatabricks.NewOutboundNetworkDependenciesEndpointsClient(sub, fakeCred{}, opts)
	if err != nil {
		t.Fatalf("outbound client: %v", err)
	}

	ob, err := obClient.List(ctx, testRG, testWS, nil)
	if err != nil {
		t.Fatalf("outbound list: %v", err)
	}

	if len(ob.OutboundEnvironmentEndpointArray) == 0 {
		t.Fatal("outbound list empty")
	}

	// ---- Provider operations (subscription-less path) ----
	opsClient, err := armdatabricks.NewOperationsClient(fakeCred{}, opts)
	if err != nil {
		t.Fatalf("operations client: %v", err)
	}

	opsPage, err := opsClient.NewListPager(nil).NextPage(ctx)
	if err != nil {
		t.Fatalf("operations list: %v", err)
	}

	if len(opsPage.Value) == 0 {
		t.Fatal("operations list empty")
	}

	// Clean teardown of the remaining sub-resources (both idempotent-capable).
	if _, err := pecClient.BeginDelete(ctx, testRG, testWS, "pec-e2e", nil); err != nil {
		t.Fatalf("PEC delete begin: %v", err)
	}

	if _, err := peerClient.BeginDelete(ctx, testRG, testWS, "peer-e2e", nil); err != nil {
		t.Fatalf("peering delete begin: %v", err)
	}
}

// delAC deletes ac-e2e and drains the poller, tolerating a not-found (idempotent).
func delAC(t *testing.T, ctx context.Context, c *armdatabricks.AccessConnectorsClient) {
	t.Helper()

	poller, err := c.BeginDelete(ctx, testRG, "ac-e2e", nil)
	if err != nil {
		t.Fatalf("AC delete begin: %v", err)
	}

	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		t.Fatalf("AC delete poll: %v", err)
	}
}
