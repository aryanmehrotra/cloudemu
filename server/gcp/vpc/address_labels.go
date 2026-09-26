package vpc

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
)

// setLabelsAction is the custom verb for POST .../addresses/{name}/setLabels
// (regional) and .../global/addresses/{name}/setLabels.
const setLabelsAction = "setLabels"

// labelsFilterPrefix is the field prefix a compute list filter uses to match a
// label value, e.g. `labels.env=prod`.
const labelsFilterPrefix = "labels."

// filterOpNotEqual is the inequality operator a compute list filter uses.
const filterOpNotEqual = "!="

// addressSetLabelsRequest is the RegionSetLabelsRequest / GlobalSetLabelsRequest
// body. Both carry the same two fields.
type addressSetLabelsRequest struct {
	Labels           map[string]string `json:"labels"`
	LabelFingerprint string            `json:"labelFingerprint"`
}

// addressLabelFingerprint returns the fingerprint of an address's label set.
// It is a pure function of the labels, so it changes exactly when they do and
// an address with no labels still has a (stable, non-empty) fingerprint the
// caller must echo back, as real Compute Engine requires.
func addressLabelFingerprint(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys)*2) //nolint:mnd // key and value per label
	for _, k := range keys {
		parts = append(parts, k, labels[k])
	}

	return fingerprintOf(append([]string{"labels"}, parts...)...)
}

// addressLabels extracts the labels map from a stored address body.
func addressLabels(body json.RawMessage) map[string]string {
	var withLabels struct {
		Labels map[string]string `json:"labels"`
	}

	_ = json.Unmarshal(body, &withLabels)

	return withLabels.Labels
}

// setAddressLabels handles setLabels on a regional or global address. The
// request's labels REPLACE the whole set; the caller must send the current
// labelFingerprint (read from a Get), and a missing or stale one is rejected
// 412 conditionNotMet with no change applied. Success returns a DONE compute
// Operation recorded in the shared registry, and a later Get shows the new
// labels under a new labelFingerprint.
//
//nolint:gocritic // rp is a request-scoped value
func (h *Handler) setAddressLabels(w http.ResponseWriter, r *http.Request, rp gcprest.ResourcePath) {
	var req addressSetLabelsRequest
	if !gcprest.DecodeJSON(w, r, &req) {
		return
	}

	scope := scopeOf(rp)

	status := h.addresses.replaceLabels(rp.Project, scope, rp.ResourceName, req)

	switch status {
	case labelsNotFound:
		gcprest.WriteError(w, http.StatusNotFound, "notFound", "address "+rp.ResourceName+" not found")
		return
	case labelsConditionNotMet:
		gcprest.WriteError(w, http.StatusPreconditionFailed, "conditionNotMet",
			"Labels fingerprint either invalid or resource labels have changed")

		return
	case labelsInvalid:
		gcprest.WriteError(w, http.StatusBadRequest, "invalid", "address body is not a JSON object")
		return
	case labelsOK:
	}

	gcprest.WriteJSON(w, http.StatusOK, h.ops.RecordDone(hostOf(r), rp.Project,
		rp.Scope, rp.ScopeName, resourceAddresses, rp.ResourceName, setLabelsAction))
}

// labelsResult is the outcome of a replaceLabels attempt.
type labelsResult int

const (
	labelsOK labelsResult = iota
	labelsNotFound
	labelsConditionNotMet
	labelsInvalid
)

// replaceLabels swaps an address's label set under the store lock, so the
// fingerprint check and the write are atomic against a concurrent setLabels.
func (s *addressStore) replaceLabels(
	project, scope, name string, req addressSetLabelsRequest,
) labelsResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, ok := s.addresses[s.key(project, scope)][name]
	if !ok {
		return labelsNotFound
	}

	if req.LabelFingerprint == "" || req.LabelFingerprint != addressLabelFingerprint(addressLabels(body)) {
		return labelsConditionNotMet
	}

	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil || obj == nil {
		return labelsInvalid
	}

	if len(req.Labels) == 0 {
		delete(obj, "labels")
	} else {
		obj["labels"] = req.Labels
	}

	obj["labelFingerprint"] = addressLabelFingerprint(req.Labels)

	out, err := json.Marshal(obj)
	if err != nil {
		return labelsInvalid
	}

	s.addresses[s.key(project, scope)][name] = out

	return labelsOK
}

// addressMatches applies a compute list filter to a stored address. It extends
// the shared name-only matcher with `labels.<key>=<value>` (and `!=`, `eq`,
// `ne`) so a label-scoped list returns only the addresses carrying that label.
// Any other field falls through to the shared matcher, which matches by name
// and treats fields it does not understand as match-all.
func addressMatches(filter string, body json.RawMessage) bool {
	f := strings.TrimSpace(filter)
	if !strings.HasPrefix(f, labelsFilterPrefix) {
		return nameMatches(filter, rawName(body))
	}

	for _, cand := range []string{filterOpNotEqual, "=", " ne ", " eq "} {
		idx := strings.Index(f, cand)
		if idx < 0 {
			continue
		}

		key := strings.TrimPrefix(strings.TrimSpace(f[:idx]), labelsFilterPrefix)
		want := strings.Trim(strings.TrimSpace(f[idx+len(cand):]), `"'`)
		op := strings.TrimSpace(cand)
		negate := op == filterOpNotEqual || op == "ne"

		got, has := addressLabels(body)[key]

		return (has && got == want) != negate
	}

	return true
}
