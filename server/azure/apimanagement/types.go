package apimanagement

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/stackshy/cloudemu/v2/internal/idgen"
	"github.com/stackshy/cloudemu/v2/providers/azure/apimanagement"
)

const (
	// skuConsumption is the serverless tier, which has no dedicated portal,
	// management or SCM endpoints and runs on the multi-tenant platform.
	skuConsumption = "Consumption"

	// platformDedicated / platformConsumption are the computePlatform versions
	// Azure reports for dedicated tiers and for the Consumption tier.
	platformDedicated   = "stv2"
	platformConsumption = "mtv1"

	// notificationSenderDefault is the sender address Azure assigns when the
	// caller sets none.
	notificationSenderDefault = "apimgmt-noreply@mail.windowsazure.com"
)

// serviceRequest is the ARM service PUT/PATCH body. location, tags, zones, sku
// and identity are top-level; the writable service properties live under
// properties and round-trip verbatim.
type serviceRequest struct {
	Location   string            `json:"location"`
	Tags       map[string]string `json:"tags,omitempty"`
	Zones      []string          `json:"zones,omitempty"`
	Sku        *skuWire          `json:"sku,omitempty"`
	Identity   *identityWire     `json:"identity,omitempty"`
	Properties json.RawMessage   `json:"properties,omitempty"`
}

// skuWire is the service SKU block. Capacity is a pointer so a request that
// omits it is distinguishable from an explicit 0 (the Consumption tier).
type skuWire struct {
	Name     string `json:"name,omitempty"`
	Capacity *int32 `json:"capacity,omitempty"`
}

// skuResponse is the SKU block as returned: capacity is always present.
type skuResponse struct {
	Name     string `json:"name"`
	Capacity int32  `json:"capacity"`
}

// identityWire is the managed-identity request/response block. On a request
// only type and userAssignedIdentities are read; on a response principalId and
// tenantId are the computed, stable values.
type identityWire struct {
	Type                   string                     `json:"type,omitempty"`
	PrincipalID            string                     `json:"principalId,omitempty"`
	TenantID               string                     `json:"tenantId,omitempty"`
	UserAssignedIdentities map[string]json.RawMessage `json:"userAssignedIdentities,omitempty"`
}

// serviceResponse is the ARM representation of an API Management service.
type serviceResponse struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Location   string            `json:"location"`
	Tags       map[string]string `json:"tags,omitempty"`
	Zones      []string          `json:"zones,omitempty"`
	Sku        skuResponse       `json:"sku"`
	Identity   *identityWire     `json:"identity,omitempty"`
	Etag       string            `json:"etag"`
	Properties json.RawMessage   `json:"properties"`
}

// serviceListResponse is the ARM service list envelope. nextLink is omitted:
// the emulator returns a single page.
type serviceListResponse struct {
	Value []serviceResponse `json:"value"`
}

// serviceInputFromRequest builds a service create/update Input from a request
// body.
func serviceInputFromRequest(req *serviceRequest) apimanagement.ServiceInput {
	in := apimanagement.ServiceInput{Tags: req.Tags, Zones: req.Zones, Properties: req.Properties}

	if req.Sku != nil {
		if req.Sku.Name != "" {
			name := req.Sku.Name
			in.SkuName = &name
		}

		in.SkuCapacity = req.Sku.Capacity
	}

	if req.Identity != nil {
		in.Identity = &apimanagement.ManagedIdentity{
			Type:            req.Identity.Type,
			UserAssignedIDs: userAssignedKeys(req.Identity.UserAssignedIdentities),
		}
	}

	return in
}

// userAssignedKeys extracts the user-assigned identity resource ids from the
// request map.
func userAssignedKeys(m map[string]json.RawMessage) []string {
	if len(m) == 0 {
		return nil
	}

	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

// toServiceResponse projects a stored service onto the ARM wire
// representation, injecting the computed read-only properties.
func toServiceResponse(s *apimanagement.Service) serviceResponse {
	return serviceResponse{
		ID:         s.ARMID(),
		Name:       s.Name,
		Type:       serviceArmType,
		Location:   s.Location,
		Tags:       s.Tags,
		Zones:      s.Zones,
		Sku:        skuResponse{Name: s.SkuName, Capacity: s.SkuCapacity},
		Identity:   toIdentityWire(s.Identity),
		Etag:       s.Etag,
		Properties: responseProperties(s),
	}
}

// responseProperties overlays the computed read-only fields onto the stored,
// verbatim properties block, filling Azure's defaults for the few writable
// fields the caller left unset.
func responseProperties(s *apimanagement.Service) json.RawMessage {
	obj := map[string]any{}
	if len(s.Properties) > 0 {
		if err := json.Unmarshal(s.Properties, &obj); err != nil {
			obj = map[string]any{}
		}
	}

	for k, v := range map[string]any{
		"virtualNetworkType":      "None",
		"publicNetworkAccess":     "Enabled",
		"notificationSenderEmail": notificationSenderDefault,
		"disableGateway":          false,
	} {
		if _, set := obj[k]; !set {
			obj[k] = v
		}
	}

	for k, v := range computedProperties(s) {
		obj[k] = v
	}

	raw, err := json.Marshal(obj)
	if err != nil {
		return s.Properties
	}

	return raw
}

// computedProperties returns the read-only properties Azure mints: the
// provisioning state, creation time, platform version and the endpoint URLs.
// The Consumption tier has only a gateway, so its portal, management and SCM
// endpoints are absent.
func computedProperties(s *apimanagement.Service) map[string]any {
	out := map[string]any{
		"provisioningState":       s.ProvisioningState,
		"targetProvisioningState": "",
		"createdAtUtc":            s.CreatedAt.UTC().Format(time.RFC3339),
		"gatewayUrl":              s.Endpoints().Gateway,
		"publicIPAddresses":       []string{},
		"platformVersion":         platformDedicated,
	}

	if s.SkuName == skuConsumption {
		out["platformVersion"] = platformConsumption

		return out
	}

	ep := s.Endpoints()
	out["gatewayRegionalUrl"] = regionalGatewayURL(s)
	out["portalUrl"] = ep.Portal
	out["developerPortalUrl"] = ep.DeveloperPortal
	out["managementApiUrl"] = ep.ManagementAPI
	out["scmUrl"] = ep.Scm

	return out
}

// regionalGatewayURL renders the primary region's gateway endpoint,
// https://<name>-<region>-01.regional.azure-api.net.
func regionalGatewayURL(s *apimanagement.Service) string {
	region := strings.ToLower(strings.ReplaceAll(s.Location, " ", ""))

	return "https://" + strings.ToLower(s.Name) + "-" + region + "-01.regional.azure-api.net"
}

// toIdentityWire projects a stored managed identity onto the wire block,
// synthesizing per-identity principal/client ids for each user-assigned entry.
func toIdentityWire(id *apimanagement.ManagedIdentity) *identityWire {
	if id == nil {
		return nil
	}

	out := &identityWire{Type: id.Type, PrincipalID: id.PrincipalID, TenantID: id.TenantID}

	if len(id.UserAssignedIDs) > 0 {
		out.UserAssignedIdentities = make(map[string]json.RawMessage, len(id.UserAssignedIDs))
		for _, uaID := range id.UserAssignedIDs {
			out.UserAssignedIdentities[uaID] = userAssignedValue(uaID)
		}
	}

	return out
}

// userAssignedValue synthesizes the deterministic {principalId, clientId}
// block Azure returns for an assigned user identity.
func userAssignedValue(uaID string) json.RawMessage {
	principal := idgen.SyntheticGUID("apimanagement/ua-principal/" + strings.ToLower(uaID))
	client := idgen.SyntheticGUID("apimanagement/ua-client/" + strings.ToLower(uaID))

	raw, err := json.Marshal(map[string]string{"principalId": principal, "clientId": client})
	if err != nil {
		return json.RawMessage(`{}`)
	}

	return raw
}
