// Package apimanagement provides an in-memory mock of Azure API Management
// (Microsoft.ApiManagement/service), the ARM control plane only. It manages the
// service lifecycle (create-or-update, get, patch, delete, list-by-group,
// list-by-subscription), the service SKU (name + capacity), availability zones
// and the system/user-assigned managed identity.
//
// The API Management data plane (the gateway that proxies traffic, the
// developer portal) and the service's child resources (apis, products,
// subscriptions, policies, named values, backends, loggers, ...) are out of
// scope, as are backup/restore, network-configuration updates and the
// soft-deleted services (deletedservices) surface.
//
// Every service-minted field stays stable for the lifetime of the resource so
// infrastructure-as-code tools (Terraform's azurerm_api_management) see no drift
// on re-plan: id/name, provisioningState ("Succeeded"), createdAtUtc, etag and
// the system-assigned identity's principalId/tenantId are minted once at create
// and byte-stable across every read and patch. The endpoint host names
// (gateway, portal, developer portal, management, scm) derive from the service
// name, exactly as Azure derives them.
//
// The writable properties block (publisherEmail, publisherName and every other
// caller-set property) is stored as raw JSON and round-trips verbatim.
package apimanagement

import (
	"context"
	"encoding/json"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stackshy/cloudemu/v2/config"
	cerrors "github.com/stackshy/cloudemu/v2/errors"
	"github.com/stackshy/cloudemu/v2/internal/idgen"
	"github.com/stackshy/cloudemu/v2/internal/memstore"
)

const (
	// providerNamespace is the ARM provider namespace.
	providerNamespace = "Microsoft.ApiManagement"
	// serviceType is the ARM service resource-type segment.
	serviceType = "service"

	// stateSucceeded is the terminal provisioningState a synchronous ARM PUT
	// reaches immediately.
	stateSucceeded = "Succeeded"

	// emulatorTenantID is the single Azure AD directory (tenant) that all
	// system-assigned identities in this emulator belong to.
	emulatorTenantID = "11111111-1111-1111-1111-111111111111"

	// hostSuffix is the DNS suffix every API Management endpoint lives under.
	hostSuffix = ".azure-api.net"
)

// ManagedIdentity is a service's top-level managed identity. For a
// system-assigned identity the PrincipalID/TenantID are synthesized once (as
// Azure mints them on assignment) and stay stable; UserAssignedIDs holds the
// assigned user-identity resource ids.
type ManagedIdentity struct {
	Type            string   `json:"type"`
	PrincipalID     string   `json:"principalId,omitempty"`
	TenantID        string   `json:"tenantId,omitempty"`
	UserAssignedIDs []string `json:"userAssignedIds,omitempty"`
}

// Service is a stored Microsoft.ApiManagement/service resource. Subscription,
// ResourceGroup and Name preserve the caller's casing; the computed fields are
// minted at create and never regenerated on a read. Properties holds the
// writable properties block and round-trips verbatim.
type Service struct {
	Subscription  string            `json:"subscription"`
	ResourceGroup string            `json:"resourceGroup"`
	Name          string            `json:"name"`
	Location      string            `json:"location"`
	Tags          map[string]string `json:"tags,omitempty"`
	Zones         []string          `json:"zones,omitempty"`

	SkuName     string           `json:"skuName"`
	SkuCapacity int32            `json:"skuCapacity"`
	Identity    *ManagedIdentity `json:"identity,omitempty"`

	Properties json.RawMessage `json:"properties,omitempty"`

	// Computed, stable fields.
	ProvisioningState string    `json:"provisioningState"`
	Etag              string    `json:"etag"`
	CreatedAt         time.Time `json:"createdAt"`
}

// ARMID returns the fully-qualified ARM resource id of the service.
func (s *Service) ARMID() string {
	return idgen.AzureID(s.Subscription, s.ResourceGroup, providerNamespace, serviceType, s.Name)
}

// Endpoints are the host URLs Azure derives from a service name.
type Endpoints struct {
	Gateway         string // https://<name>.azure-api.net (the proxy)
	Portal          string // legacy publisher portal
	DeveloperPortal string // developer portal
	ManagementAPI   string // direct management REST endpoint
	Scm             string // git configuration (SCM) endpoint
}

// Endpoints returns the service's endpoint URLs.
func (s *Service) Endpoints() Endpoints {
	return Endpoints{
		Gateway:         s.endpoint(""),
		Portal:          s.endpoint(".portal"),
		DeveloperPortal: s.endpoint(".developer"),
		ManagementAPI:   s.endpoint(".management"),
		Scm:             s.endpoint(".scm"),
	}
}

// endpoint renders https://<name><infix>.azure-api.net. Azure host names are
// lowercase whatever casing the caller used for the service name.
func (s *Service) endpoint(infix string) string {
	return "https://" + strings.ToLower(s.Name) + infix + hostSuffix
}

// ServiceInput carries the mutable fields of a service create/update request.
// A nil pointer/map/slice means "not supplied": on a PATCH the stored value is
// preserved, so the request overlays only what it names.
type ServiceInput struct {
	Tags        map[string]string
	Zones       []string
	SkuName     *string
	SkuCapacity *int32
	Identity    *ManagedIdentity
	Properties  json.RawMessage
}

// Mock is the in-memory backend for API Management services.
type Mock struct {
	mu       sync.RWMutex
	clock    config.Clock
	services *memstore.Store[*Service]
}

// New creates an empty API Management mock. It falls back to the real clock
// when opts (or its clock) is nil so the mock stays usable standalone.
func New(opts *config.Options) *Mock {
	clock := config.Clock(config.RealClock{})
	if opts != nil && opts.Clock != nil {
		clock = opts.Clock
	}

	return &Mock{clock: clock, services: memstore.New[*Service]()}
}

// serviceKey is the case-insensitive store key for a service.
func serviceKey(sub, rg, name string) string {
	return strings.ToLower(idgen.AzureID(sub, rg, providerNamespace, serviceType, name))
}

// CreateOrUpdateService creates a new service or replaces an existing one (ARM
// PUT semantics: tags, zones, identity and the properties block are replaced
// wholesale). The computed fields (provisioningState, etag, createdAtUtc) are
// minted once at create and preserved across updates; location is immutable in
// real Azure and is preserved on update. It returns the stored service and
// whether it was newly created.
func (m *Mock) CreateOrUpdateService(
	_ context.Context, sub, rg, name, location string, in *ServiceInput,
) (Service, bool, error) {
	if err := validateCreate(sub, rg, name, location, in); err != nil {
		return Service{}, false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := serviceKey(sub, rg, name)

	existing, existed := m.services.Get(k)

	var s Service
	if existed {
		s = *existing
	} else {
		s = m.newService(sub, rg, name, location)
	}

	s.Tags = maps.Clone(in.Tags)
	s.Zones = append([]string(nil), in.Zones...)
	s.SkuName = canonicalSKU(*in.SkuName)
	s.SkuCapacity = *in.SkuCapacity
	s.Identity = resolveIdentity(in.Identity, sub, rg, name)
	s.Properties = append(json.RawMessage(nil), in.Properties...)

	m.services.Set(k, &s)

	return cloneService(&s), !existed, nil
}

// UpdateService applies an ARM PATCH: tags and zones are replaced wholesale
// when supplied, sku/identity are re-resolved only when supplied, and the
// properties block is merged key-by-key onto the stored block. The merged
// result is re-validated, so a PATCH cannot blank the publisher fields or leave
// an invalid SKU/capacity pair. A PATCH on a missing service is a NotFound.
func (m *Mock) UpdateService(_ context.Context, sub, rg, name string, in *ServiceInput) (Service, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := serviceKey(sub, rg, name)

	existing, ok := m.services.Get(k)
	if !ok {
		return Service{}, notFound(name)
	}

	s := *existing
	applyPatch(&s, in, sub, rg, name)

	if err := validateSKU(s.SkuName, s.SkuCapacity); err != nil {
		return Service{}, err
	}

	if err := validatePublisher(s.Properties); err != nil {
		return Service{}, err
	}

	m.services.Set(k, &s)

	return cloneService(&s), nil
}

// applyPatch overlays the supplied PATCH fields onto s. A nil pointer/map/slice
// preserves the stored value.
func applyPatch(s *Service, in *ServiceInput, sub, rg, name string) {
	if in.Tags != nil {
		s.Tags = maps.Clone(in.Tags)
	}

	if in.Zones != nil {
		s.Zones = append([]string(nil), in.Zones...)
	}

	if in.SkuName != nil {
		s.SkuName = canonicalSKU(*in.SkuName)
	}

	if in.SkuCapacity != nil {
		s.SkuCapacity = *in.SkuCapacity
	}

	if in.Identity != nil {
		s.Identity = resolveIdentity(in.Identity, sub, rg, name)
	}

	if in.Properties != nil {
		s.Properties = mergeRaw(s.Properties, in.Properties)
	}
}

// newService seeds a fresh service with its immutable identity and its
// computed, stable fields. The etag derives deterministically from the resource
// id so it is stable yet distinct per service.
func (m *Mock) newService(sub, rg, name, location string) Service {
	return Service{
		Subscription:      sub,
		ResourceGroup:     rg,
		Name:              name,
		Location:          location,
		ProvisioningState: stateSucceeded,
		Etag:              idgen.SyntheticGUID("apimanagement/etag/" + serviceKey(sub, rg, name)),
		CreatedAt:         m.clock.Now().UTC().Truncate(time.Second),
	}
}

// GetService returns the service, or a NotFound error.
func (m *Mock) GetService(_ context.Context, sub, rg, name string) (Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.services.Get(serviceKey(sub, rg, name))
	if !ok {
		return Service{}, notFound(name)
	}

	return cloneService(s), nil
}

// DeleteService removes the service, reporting whether it existed.
func (m *Mock) DeleteService(_ context.Context, sub, rg, name string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.services.Delete(serviceKey(sub, rg, name)), nil
}

// ListServicesByResourceGroup returns every service in the group, sorted by
// name.
func (m *Mock) ListServicesByResourceGroup(_ context.Context, sub, rg string) ([]Service, error) {
	return m.filterServices(func(s *Service) bool {
		return strings.EqualFold(s.Subscription, sub) && strings.EqualFold(s.ResourceGroup, rg)
	}), nil
}

// ListServicesBySubscription returns every service in the subscription, sorted
// by name.
func (m *Mock) ListServicesBySubscription(_ context.Context, sub string) ([]Service, error) {
	return m.filterServices(func(s *Service) bool {
		return strings.EqualFold(s.Subscription, sub)
	}), nil
}

// DiscoverServices returns every stored service, for the inventory walk.
func (m *Mock) DiscoverServices(_ context.Context) ([]Service, error) {
	return m.filterServices(func(*Service) bool { return true }), nil
}

// PurgeResourceGroup deletes every service under sub/rg, so a resource-group
// delete cascades into its API Management services.
func (m *Mock) PurgeResourceGroup(_ context.Context, sub, rg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, s := range m.services.All() {
		if strings.EqualFold(s.Subscription, sub) && strings.EqualFold(s.ResourceGroup, rg) {
			m.services.Delete(k)
		}
	}

	return nil
}

// filterServices returns the services matching pred, sorted by name.
func (m *Mock) filterServices(pred func(*Service) bool) []Service {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []Service

	for _, s := range m.services.All() {
		if pred(s) {
			out = append(out, cloneService(s))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// resolveIdentity normalizes an incoming managed identity: for a
// system-assigned identity it synthesizes deterministic principal/tenant GUIDs
// (as Azure does on assignment); a nil or "None" identity resolves to nil.
func resolveIdentity(in *ManagedIdentity, sub, rg, name string) *ManagedIdentity {
	if in == nil || in.Type == "" || strings.EqualFold(in.Type, "None") {
		return nil
	}

	out := &ManagedIdentity{
		Type:            in.Type,
		UserAssignedIDs: append([]string(nil), in.UserAssignedIDs...),
	}
	sort.Strings(out.UserAssignedIDs)

	if strings.Contains(strings.ToLower(in.Type), "systemassigned") {
		// Keyed on the full resource id so two services with the same name in
		// different groups stay distinct, while the value is stable across
		// gets/patches/restarts for the same service.
		out.PrincipalID = idgen.SyntheticGUID("apimanagement/principal/" + serviceKey(sub, rg, name))
		out.TenantID = emulatorTenantID
	}

	return out
}

// notFound is the NotFound error for a missing service.
func notFound(name string) error {
	return cerrors.Newf(cerrors.NotFound, "API Management service %q not found", name)
}

// cloneService deep-copies a stored service so callers never alias the store.
func cloneService(s *Service) Service {
	out := *s
	out.Tags = maps.Clone(s.Tags)
	out.Zones = append([]string(nil), s.Zones...)
	out.Identity = cloneIdentity(s.Identity)

	if s.Properties != nil {
		out.Properties = append(json.RawMessage(nil), s.Properties...)
	}

	return out
}

// cloneIdentity deep-copies a managed identity, or returns nil.
func cloneIdentity(id *ManagedIdentity) *ManagedIdentity {
	if id == nil {
		return nil
	}

	out := *id
	out.UserAssignedIDs = append([]string(nil), id.UserAssignedIDs...)

	return &out
}

// mergeRaw overlays the top-level keys of patch onto base and returns the
// merged raw JSON object. A malformed base or patch falls back to whichever
// side parses, so a merge never drops the caller's bytes silently.
func mergeRaw(base, patch json.RawMessage) json.RawMessage {
	merged := map[string]json.RawMessage{}
	if len(base) > 0 {
		if err := json.Unmarshal(base, &merged); err != nil {
			merged = map[string]json.RawMessage{}
		}
	}

	overlay := map[string]json.RawMessage{}
	if err := json.Unmarshal(patch, &overlay); err != nil {
		return append(json.RawMessage(nil), patch...)
	}

	maps.Copy(merged, overlay)

	raw, err := json.Marshal(merged)
	if err != nil {
		return append(json.RawMessage(nil), patch...)
	}

	return raw
}
