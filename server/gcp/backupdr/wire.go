package backupdr

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
	bdrdriver "github.com/stackshy/cloudemu/v2/services/backupdr/driver"
)

// maxBodyBytes caps a decoded request body.
const maxBodyBytes = 8 << 20

// decimalBase formats the int64-as-string output fields.
const decimalBase = 10

// encryptionJSON mirrors backupdr/v1 EncryptionConfig.
type encryptionJSON struct {
	KmsKeyName string `json:"kmsKeyName,omitempty"`
}

// vaultInput is the caller-settable subset of a backupdr/v1 BackupVault body.
// Output-only keys (state, serviceAccount, uid, …) are simply not decoded, so a
// caller cannot pin them. Etag is read for the patch concurrency check.
type vaultInput struct {
	Name                                   string            `json:"name"`
	Description                            string            `json:"description"`
	Labels                                 map[string]string `json:"labels"`
	Annotations                            map[string]string `json:"annotations"`
	BackupMinimumEnforcedRetentionDuration string            `json:"backupMinimumEnforcedRetentionDuration"`
	BackupRetentionInheritance             string            `json:"backupRetentionInheritance"`
	EffectiveTime                          string            `json:"effectiveTime"`
	AccessRestriction                      string            `json:"accessRestriction"`
	EncryptionConfig                       *encryptionJSON   `json:"encryptionConfig"`
	Etag                                   string            `json:"etag"`
}

// toConfig maps the decoded body onto a driver config for the given identity.
func (in *vaultInput) toConfig(project, location, id string) *bdrdriver.BackupVaultConfig {
	cfg := &bdrdriver.BackupVaultConfig{
		Project:                                project,
		Location:                               location,
		ID:                                     id,
		Description:                            in.Description,
		Labels:                                 in.Labels,
		Annotations:                            in.Annotations,
		BackupMinimumEnforcedRetentionDuration: in.BackupMinimumEnforcedRetentionDuration,
		BackupRetentionInheritance:             in.BackupRetentionInheritance,
		EffectiveTime:                          in.EffectiveTime,
		AccessRestriction:                      in.AccessRestriction,
	}

	if in.EncryptionConfig != nil {
		cfg.EncryptionConfig = &bdrdriver.EncryptionConfig{KmsKeyName: in.EncryptionConfig.KmsKeyName}
	}

	return cfg
}

// vaultJSON is the backupdr/v1 BackupVault wire shape. backupCount and
// totalStoredBytes are int64 fields, which the JSON mapping renders as strings
// (the SDK decodes them with `json:",string"`); they and deletable are always
// emitted so a client sees "0"/true on an empty vault.
type vaultJSON struct {
	Name                                   string            `json:"name"`
	Description                            string            `json:"description,omitempty"`
	Labels                                 map[string]string `json:"labels,omitempty"`
	Annotations                            map[string]string `json:"annotations,omitempty"`
	BackupMinimumEnforcedRetentionDuration string            `json:"backupMinimumEnforcedRetentionDuration,omitempty"`
	BackupRetentionInheritance             string            `json:"backupRetentionInheritance,omitempty"`
	EffectiveTime                          string            `json:"effectiveTime,omitempty"`
	AccessRestriction                      string            `json:"accessRestriction,omitempty"`
	EncryptionConfig                       *encryptionJSON   `json:"encryptionConfig,omitempty"`
	State                                  string            `json:"state"`
	ServiceAccount                         string            `json:"serviceAccount"`
	UID                                    string            `json:"uid"`
	Etag                                   string            `json:"etag"`
	Deletable                              bool              `json:"deletable"`
	BackupCount                            string            `json:"backupCount"`
	TotalStoredBytes                       string            `json:"totalStoredBytes"`
	CreateTime                             string            `json:"createTime"`
	UpdateTime                             string            `json:"updateTime"`
}

// vaultAny is a vault wrapped as a google.protobuf.Any, the shape a completed
// operation's `response` carries.
type vaultAny struct {
	Type string `json:"@type"`
	vaultJSON
}

// listJSON is ListBackupVaultsResponse.
type listJSON struct {
	BackupVaults  []vaultJSON `json:"backupVaults"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// operationJSON mirrors google.longrunning.Operation. Mutating ops complete
// inline, so `done` is always true; `response` carries the resulting vault (an
// Any for create/patch, absent for delete).
type operationJSON struct {
	Name     string          `json:"name"`
	Done     bool            `json:"done"`
	Response json.RawMessage `json:"response,omitempty"`
}

// decodeVault reads the request body once into the caller-settable fields.
func decodeVault(w http.ResponseWriter, r *http.Request) (*vaultInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		gcprest.WriteError(w, http.StatusBadRequest, "invalid", "reading request body: "+err.Error())
		return nil, false
	}

	in := &vaultInput{}

	if len(raw) > 0 {
		if err := json.Unmarshal(raw, in); err != nil {
			gcprest.WriteError(w, http.StatusBadRequest, "invalid", "malformed JSON body: "+err.Error())
			return nil, false
		}
	}

	return in, true
}

// toVaultJSON renders a driver vault as backupdr/v1 wire JSON.
func toVaultJSON(v *bdrdriver.BackupVault) vaultJSON {
	out := vaultJSON{
		Name:                                   resourceName(v.Project, v.Location, v.ID),
		Description:                            v.Description,
		Labels:                                 v.Labels,
		Annotations:                            v.Annotations,
		BackupMinimumEnforcedRetentionDuration: v.BackupMinimumEnforcedRetentionDuration,
		BackupRetentionInheritance:             v.BackupRetentionInheritance,
		EffectiveTime:                          v.EffectiveTime,
		AccessRestriction:                      v.AccessRestriction,
		State:                                  v.State,
		ServiceAccount:                         v.ServiceAccount,
		UID:                                    v.UID,
		Etag:                                   v.Etag,
		Deletable:                              v.Deletable(),
		BackupCount:                            strconv.FormatInt(v.BackupCount, decimalBase),
		TotalStoredBytes:                       strconv.FormatInt(v.TotalStoredBytes, decimalBase),
		CreateTime:                             formatTime(v.CreateTime),
		UpdateTime:                             formatTime(v.UpdateTime),
	}

	if v.EncryptionConfig != nil {
		out.EncryptionConfig = &encryptionJSON{KmsKeyName: v.EncryptionConfig.KmsKeyName}
	}

	return out
}

// writeVaultOperation writes a completed operation carrying the vault as its
// Any-typed response (create/patch).
func (h *Handler) writeVaultOperation(w http.ResponseWriter, op *bdrdriver.Operation, v *bdrdriver.BackupVault) {
	raw, err := json.Marshal(vaultAny{Type: vaultTypeURL, vaultJSON: toVaultJSON(v)})
	if err != nil {
		gcprest.WriteError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	gcprest.WriteJSON(w, http.StatusOK, h.doneOperation(op.Name, raw))
}

// doneOperation builds a completed google.longrunning.Operation and records it
// with the shared LRO poller (a no-op on a nil registry) so a client polling
// the returned name resolves the same done operation (with its response).
func (h *Handler) doneOperation(name string, resp json.RawMessage) operationJSON {
	if h.ops != nil {
		// A nil RawMessage stored in the registry's `any` would be a non-nil
		// interface and replay as "response": null; register a true nil instead.
		if resp == nil {
			h.ops.Register(name, nil)
		} else {
			h.ops.Register(name, resp)
		}
	}

	return operationJSON{Name: name, Done: true, Response: resp}
}

// writeErr maps a driver error onto the Google JSON error envelope. A stale
// etag is 409 ABORTED; a vault that still holds backups is FAILED_PRECONDITION,
// which the Google REST mapping renders as HTTP 400; everything else follows
// the shared cerrors mapping.
func writeErr(w http.ResponseWriter, err error) {
	msg := cerrors.Message(err)

	switch {
	case errors.Is(err, bdrdriver.ErrEtagMismatch):
		gcprest.WriteError(w, http.StatusConflict, "ABORTED", msg)
	case cerrors.IsFailedPrecondition(err):
		gcprest.WriteError(w, http.StatusBadRequest, "FAILED_PRECONDITION", msg)
	default:
		gcprest.WriteCErr(w, err)
	}
}

// formatTime renders t as RFC3339Nano; a zero time renders as the empty string.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339Nano)
}
