package backupdr

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/stackshy/cloudemu/v2/internal/pagination"
	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
	bdrdriver "github.com/stackshy/cloudemu/v2/services/backupdr/driver"
)

const (
	defaultPageSize = 500
	maxPageSize     = 500

	paramValidateOnly = "validateOnly"
	paramForce        = "force"
	paramAllowMissing = "allowMissing"
	paramEtag         = "etag"
	paramUpdateMask   = "updateMask"
)

// createVault handles POST .../backupVaults?backupVaultId=. The id is the query
// param, falling back to the trailing segment of the body name. requestId is
// accepted and ignored (every CloudEmu create completes inline, so there is no
// in-flight request to deduplicate); validateOnly runs every check without
// storing the vault.
func (h *Handler) createVault(w http.ResponseWriter, r *http.Request, rt route) {
	in, ok := decodeVault(w, r)
	if !ok {
		return
	}

	validateOnly, ok := boolParam(w, r, paramValidateOnly)
	if !ok {
		return
	}

	id := r.URL.Query().Get(vaultIDParam)
	if id == "" {
		id = lastSegment(in.Name)
	}

	if id == "" {
		gcprest.WriteError(w, http.StatusBadRequest, "invalidArgument", vaultIDParam+" is required")
		return
	}

	cfg := in.toConfig(rt.project, rt.location, id)
	cfg.ValidateOnly = validateOnly

	v, op, err := h.db.CreateBackupVault(r.Context(), cfg)
	if err != nil {
		writeErr(w, err)
		return
	}

	h.writeVaultOperation(w, op, v)
}

// getVault handles GET .../backupVaults/{id}. The view param is accepted and
// ignored: BASIC and FULL render identically for a vault with no data sources.
func (h *Handler) getVault(w http.ResponseWriter, r *http.Request, rt route) {
	v, err := h.db.GetBackupVault(r.Context(), rt.project, rt.location, rt.name)
	if err != nil {
		writeErr(w, err)
		return
	}

	gcprest.WriteJSON(w, http.StatusOK, toVaultJSON(v))
}

// listVaults handles GET .../backupVaults, scoped to the request's project and
// location ("-" for every location) and ordered by resource name. filter and
// orderBy are accepted and ignored.
func (h *Handler) listVaults(w http.ResponseWriter, r *http.Request, rt route) {
	all, err := h.db.ListBackupVaults(r.Context(), rt.project, rt.location)
	if err != nil {
		writeErr(w, err)
		return
	}

	page, err := pagination.PaginateSorted(all,
		func(a, b bdrdriver.BackupVault) bool {
			return resourceName(a.Project, a.Location, a.ID) < resourceName(b.Project, b.Location, b.ID)
		},
		r.URL.Query().Get("pageToken"), pageSize(r))
	if err != nil {
		gcprest.WriteError(w, http.StatusBadRequest, "invalid", "invalid pageToken")
		return
	}

	items := make([]vaultJSON, 0, len(page.Items))
	for i := range page.Items {
		items = append(items, toVaultJSON(&page.Items[i]))
	}

	gcprest.WriteJSON(w, http.StatusOK, listJSON{BackupVaults: items, NextPageToken: page.NextPageToken})
}

// patchVault handles PATCH .../backupVaults/{id}?updateMask=. The mask is
// required; only masked fields change. A body etag, when present, must match
// the stored vault (409 ABORTED otherwise). force, forceUpdateAccessRestriction
// and requestId are accepted and ignored (there are no backup plans or data
// sources to check against).
func (h *Handler) patchVault(w http.ResponseWriter, r *http.Request, rt route) {
	in, ok := decodeVault(w, r)
	if !ok {
		return
	}

	validateOnly, ok := boolParam(w, r, paramValidateOnly)
	if !ok {
		return
	}

	cfg := in.toConfig(rt.project, rt.location, rt.name)
	cfg.Etag = in.Etag
	cfg.ValidateOnly = validateOnly

	v, op, err := h.db.UpdateBackupVault(r.Context(), cfg, parseMask(r.URL.Query().Get(paramUpdateMask)))
	if err != nil {
		writeErr(w, err)
		return
	}

	h.writeVaultOperation(w, op, v)
}

// deleteVault handles DELETE .../backupVaults/{id}. force, allowMissing, etag
// and validateOnly are honored; ignoreBackupPlanReferences and requestId are
// accepted and ignored (there are no backup plans). The operation completes
// inline with an empty response.
func (h *Handler) deleteVault(w http.ResponseWriter, r *http.Request, rt route) {
	req := &bdrdriver.DeleteBackupVaultRequest{
		Project: rt.project, Location: rt.location, ID: rt.name,
		Etag: r.URL.Query().Get(paramEtag),
	}

	flags := []struct {
		param string
		dst   *bool
	}{
		{paramForce, &req.Force},
		{paramAllowMissing, &req.AllowMissing},
		{paramValidateOnly, &req.ValidateOnly},
	}

	for _, f := range flags {
		v, ok := boolParam(w, r, f.param)
		if !ok {
			return
		}

		*f.dst = v
	}

	op, err := h.db.DeleteBackupVault(r.Context(), req)
	if err != nil {
		writeErr(w, err)
		return
	}

	gcprest.WriteJSON(w, http.StatusOK, h.doneOperation(op.Name, nil))
}

// serveOperation resolves a (done) long-running operation poll for a
// standalone package server (no shared registry). The operation resource name
// is the request path without the /v1/ version prefix.
func (h *Handler) serveOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/v1/")

	op, err := h.db.GetOperation(r.Context(), name)
	if err != nil {
		writeErr(w, err)
		return
	}

	gcprest.WriteJSON(w, http.StatusOK, operationJSON{Name: op.Name, Done: true})
}

// boolParam reads an optional boolean query parameter; a malformed value is a
// 400.
func boolParam(w http.ResponseWriter, r *http.Request, name string) (value, ok bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return false, true
	}

	v, err := strconv.ParseBool(raw)
	if err != nil {
		gcprest.WriteError(w, http.StatusBadRequest, "invalidArgument", "invalid boolean for "+name+": "+raw)
		return false, false
	}

	return v, true
}

// parseMask splits a comma-separated updateMask query param into field paths.
func parseMask(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

// lastSegment returns the trailing path segment of a resource name.
func lastSegment(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}

	return name
}

// pageSize reads ?pageSize, clamping to a sane default and ceiling.
func pageSize(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if err != nil || n <= 0 {
		return defaultPageSize
	}

	if n > maxPageSize {
		return maxPageSize
	}

	return n
}
