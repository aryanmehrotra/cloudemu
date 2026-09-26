// Package apimanagement serves the Azure API Management service ARM API
// (Microsoft.ApiManagement/service). Real armapimanagement ServiceClient
// requests hit this handler the same way they hit management.azure.com.
//
// Real Azure runs service create, update and delete as long-running operations
// (a Developer-tier create takes 30-45 minutes). The emulator completes them
// synchronously: PUT answers 201/200 and PATCH 200 with a body whose
// properties.provisioningState is already "Succeeded" and no
// Azure-AsyncOperation / Location header, which azcore's Body poller treats as
// terminal on the initial response; DELETE answers 200/204 with no polling
// header, which azcore treats as a completed no-op poll. So PollUntilDone
// returns on the first call.
//
// The service's child resources (apis, products, subscriptions, policies,
// backends, ...), the gateway data plane, backup/restore and the soft-deleted
// services (deletedservices) surface are out of scope.
package apimanagement

import (
	"context"
	"net/http"
	"strings"

	"github.com/stackshy/cloudemu/v2/providers/azure/apimanagement"
	"github.com/stackshy/cloudemu/v2/server/wire/azurearm"
)

const (
	providerName   = "Microsoft.ApiManagement"
	serviceType    = "service"
	serviceArmType = providerName + "/" + serviceType
)

// Store is the minimal API Management backend the handler needs.
// *apimanagement.Mock satisfies it.
type Store interface {
	CreateOrUpdateService(
		ctx context.Context, sub, rg, name, location string, in *apimanagement.ServiceInput,
	) (apimanagement.Service, bool, error)
	UpdateService(ctx context.Context, sub, rg, name string, in *apimanagement.ServiceInput) (apimanagement.Service, error)
	GetService(ctx context.Context, sub, rg, name string) (apimanagement.Service, error)
	DeleteService(ctx context.Context, sub, rg, name string) (bool, error)
	ListServicesByResourceGroup(ctx context.Context, sub, rg string) ([]apimanagement.Service, error)
	ListServicesBySubscription(ctx context.Context, sub string) ([]apimanagement.Service, error)
	PurgeResourceGroup(ctx context.Context, sub, rg string) error
}

// Handler serves Microsoft.ApiManagement/service ARM requests.
type Handler struct {
	store Store
}

// New returns an API Management handler backed by store.
func New(store Store) *Handler {
	return &Handler{store: store}
}

// Matches reports whether r targets an API Management service ARM URL. The
// provider and type are matched case-insensitively.
func (*Handler) Matches(r *http.Request) bool {
	rp, ok := azurearm.ParsePath(r.URL.Path)
	if !ok {
		return false
	}

	return strings.EqualFold(rp.Provider, providerName) &&
		strings.EqualFold(rp.ResourceType, serviceType)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rp, ok := azurearm.ParsePath(r.URL.Path)
	if !ok {
		azurearm.WriteError(w, http.StatusBadRequest, "InvalidPath", "malformed ARM path")
		return
	}

	switch {
	case rp.ResourceName == "":
		h.listServices(w, r, &rp)
	case rp.SubResource == "":
		h.serveService(w, r, &rp)
	default:
		azurearm.WriteError(w, http.StatusNotFound, "InvalidResourceType",
			"unsupported API Management sub-resource "+rp.SubResource)
	}
}

// PurgeResourceGroup deletes every service under sub/rg so a resource-group
// delete cascades into them.
func (h *Handler) PurgeResourceGroup(ctx context.Context, subscription, resourceGroup string) error {
	return h.store.PurgeResourceGroup(ctx, subscription, resourceGroup)
}

// serveService routes the top-level service CRUD surface.
func (h *Handler) serveService(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	switch r.Method {
	case http.MethodPut:
		h.createService(w, r, rp)
	case http.MethodPatch:
		h.updateService(w, r, rp)
	case http.MethodGet:
		h.getService(w, r, rp)
	case http.MethodDelete:
		h.deleteService(w, r, rp)
	default:
		azurearm.WriteError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (h *Handler) createService(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	if rp.ResourceGroup == "" {
		azurearm.WriteError(w, http.StatusBadRequest, "InvalidPath", "missing resourceGroups segment")
		return
	}

	var req serviceRequest
	if !azurearm.DecodeJSON(w, r, &req) {
		return
	}

	in := serviceInputFromRequest(&req)

	s, created, err := h.store.CreateOrUpdateService(
		r.Context(), rp.Subscription, rp.ResourceGroup, rp.ResourceName, req.Location, &in)
	if err != nil {
		azurearm.WriteCErr(w, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}

	azurearm.WriteJSON(w, status, toServiceResponse(&s))
}

// updateService applies an ARM PATCH: supplied tags replace the set, sku and
// identity are re-resolved when named, and the properties block is merged. A
// PATCH on a missing service is a 404.
func (h *Handler) updateService(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	var req serviceRequest
	if !azurearm.DecodeJSON(w, r, &req) {
		return
	}

	in := serviceInputFromRequest(&req)

	s, err := h.store.UpdateService(r.Context(), rp.Subscription, rp.ResourceGroup, rp.ResourceName, &in)
	if err != nil {
		azurearm.WriteCErr(w, err)
		return
	}

	azurearm.WriteJSON(w, http.StatusOK, toServiceResponse(&s))
}

func (h *Handler) getService(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	s, err := h.store.GetService(r.Context(), rp.Subscription, rp.ResourceGroup, rp.ResourceName)
	if err != nil {
		azurearm.WriteCErr(w, err)
		return
	}

	azurearm.WriteJSON(w, http.StatusOK, toServiceResponse(&s))
}

// deleteService is the idempotent ARM DELETE: 200 when the service existed,
// 204 when it did not.
func (h *Handler) deleteService(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	existed, err := h.store.DeleteService(r.Context(), rp.Subscription, rp.ResourceGroup, rp.ResourceName)
	if err != nil {
		azurearm.WriteCErr(w, err)
		return
	}

	if existed {
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listServices(w http.ResponseWriter, r *http.Request, rp *azurearm.ResourcePath) {
	if r.Method != http.MethodGet {
		azurearm.WriteError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
		return
	}

	var (
		items []apimanagement.Service
		err   error
	)

	if rp.ResourceGroup != "" {
		items, err = h.store.ListServicesByResourceGroup(r.Context(), rp.Subscription, rp.ResourceGroup)
	} else {
		items, err = h.store.ListServicesBySubscription(r.Context(), rp.Subscription)
	}

	if err != nil {
		azurearm.WriteCErr(w, err)
		return
	}

	out := serviceListResponse{Value: make([]serviceResponse, 0, len(items))}
	for i := range items {
		out.Value = append(out.Value, toServiceResponse(&items[i]))
	}

	azurearm.WriteJSON(w, http.StatusOK, out)
}
