package backupdr

import (
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
	"time"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
	bdrdriver "github.com/stackshy/cloudemu/v2/services/backupdr/driver"
)

const (
	stateActive = "ACTIVE"

	accessWithinOrganization = "WITHIN_ORGANIZATION"
	accessUnspecified        = "ACCESS_RESTRICTION_UNSPECIFIED"

	// minVaultIDLen / maxVaultIDLen bound a vault id, per the BackupVault.name
	// contract ("must be between 3-63 characters long").
	minVaultIDLen = 3
	maxVaultIDLen = 63

	// serviceAccountDomain is the Backup and DR service-agent domain. CloudEmu
	// synthesizes the vault serviceAccount as
	// service-{projectNumber}@gcp-sa-backupdr-pr.iam.gserviceaccount.com, where
	// projectNumber is a stable 12-digit number derived from the project id (see
	// projectNumber), so it is identical on every read and across restarts.
	serviceAccountDomain = "@gcp-sa-backupdr-pr.iam.gserviceaccount.com"

	// projectNumberBase / projectNumberSpan keep a derived project number at
	// exactly 12 digits (100000000000..999999999999), like a real GCP project
	// number.
	projectNumberBase = 100000000000
	projectNumberSpan = 900000000000

	fieldDescription      = "description"
	fieldLabels           = "labels"
	fieldAnnotations      = "annotations"
	fieldRetention        = "backupMinimumEnforcedRetentionDuration"
	fieldInheritance      = "backupRetentionInheritance"
	fieldEffectiveTime    = "effectiveTime"
	fieldAccess           = "accessRestriction"
	fieldEncryptionCfg    = "encryptionConfig"
	etagRevisionSeparator = "#"

	// hexBase / decimalBase are the strconv bases for the etag and numbers.
	hexBase     = 16
	decimalBase = 10
)

// durationPattern matches the google.protobuf.Duration JSON form: an optionally
// signed decimal number of seconds with up to nine fractional digits and an "s"
// suffix (e.g. "86400s", "1.5s", "-3s").
var durationPattern = regexp.MustCompile(`^(-)?\d+(\.\d{1,9})?s$`)

// validAccessRestrictions is the AccessRestriction enum from the discovery doc.
//
//nolint:gochecknoglobals // immutable lookup set
var validAccessRestrictions = map[string]bool{
	accessUnspecified:                    true,
	"WITHIN_PROJECT":                     true,
	accessWithinOrganization:             true,
	"UNRESTRICTED":                       true,
	"WITHIN_ORG_BUT_UNRESTRICTED_FOR_BA": true,
}

// validInheritance is the BackupRetentionInheritance enum from the discovery doc.
//
//nolint:gochecknoglobals // immutable lookup set
var validInheritance = map[string]bool{
	"BACKUP_RETENTION_INHERITANCE_UNSPECIFIED": true,
	"INHERIT_VAULT_RETENTION":                  true,
	"MATCH_BACKUP_EXPIRE_TIME":                 true,
}

// outputOnlyFields are BackupVault fields a caller may not name in an
// updateMask: they are output-only (or, for name, immutable identity).
//
//nolint:gochecknoglobals // immutable lookup set
var outputOnlyFields = map[string]bool{
	"name": true, "createTime": true, "updateTime": true, "state": true,
	"deletable": true, "etag": true, "serviceAccount": true, "uid": true,
	"totalStoredBytes": true, "backupCount": true,
}

// mutableFields are the BackupVault fields an updateMask may name.
//
//nolint:gochecknoglobals // immutable lookup set
var mutableFields = map[string]bool{
	fieldDescription: true, fieldLabels: true, fieldAnnotations: true, fieldRetention: true,
	fieldInheritance: true, fieldEffectiveTime: true, fieldAccess: true, fieldEncryptionCfg: true,
}

// validateCreate checks a create request: the vault id and the required
// retention duration, plus every optional enum/timestamp the caller supplied.
func validateCreate(cfg *bdrdriver.BackupVaultConfig) error {
	if cfg.Location == "" || cfg.Location == anyLocation {
		return cerrors.New(cerrors.InvalidArgument, "a concrete location is required")
	}

	if n := len(cfg.ID); n < minVaultIDLen || n > maxVaultIDLen {
		return cerrors.Newf(cerrors.InvalidArgument,
			"backupVaultId %q must be between %d and %d characters", cfg.ID, minVaultIDLen, maxVaultIDLen)
	}

	checks := []func(*bdrdriver.BackupVaultConfig) error{
		checkRetention, checkInheritance, checkEffectiveTime, checkAccessRestriction,
	}

	for _, check := range checks {
		if err := check(cfg); err != nil {
			return err
		}
	}

	return nil
}

// checkRetention requires backupMinimumEnforcedRetentionDuration to be a
// well-formed, non-negative google.protobuf.Duration string.
func checkRetention(cfg *bdrdriver.BackupVaultConfig) error {
	d := cfg.BackupMinimumEnforcedRetentionDuration
	if d == "" {
		return cerrors.New(cerrors.InvalidArgument, "backupMinimumEnforcedRetentionDuration is required")
	}

	m := durationPattern.FindStringSubmatch(d)
	if m == nil {
		return cerrors.Newf(cerrors.InvalidArgument,
			"backupMinimumEnforcedRetentionDuration %q is not a valid duration (want e.g. \"86400s\")", d)
	}

	if m[1] != "" {
		if secs, err := strconv.ParseFloat(strings.TrimSuffix(d, "s"), 64); err != nil || secs != 0 {
			return cerrors.Newf(cerrors.InvalidArgument, "backupMinimumEnforcedRetentionDuration %q must not be negative", d)
		}
	}

	return nil
}

// checkInheritance validates an optional backupRetentionInheritance enum.
func checkInheritance(cfg *bdrdriver.BackupVaultConfig) error {
	if v := cfg.BackupRetentionInheritance; v != "" && !validInheritance[v] {
		return cerrors.Newf(cerrors.InvalidArgument, "invalid backupRetentionInheritance %q", v)
	}

	return nil
}

// checkEffectiveTime validates an optional RFC 3339 effectiveTime.
func checkEffectiveTime(cfg *bdrdriver.BackupVaultConfig) error {
	if v := cfg.EffectiveTime; v != "" {
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			return cerrors.Newf(cerrors.InvalidArgument, "effectiveTime %q is not an RFC 3339 timestamp", v)
		}
	}

	return nil
}

// checkAccessRestriction validates an optional accessRestriction enum.
func checkAccessRestriction(cfg *bdrdriver.BackupVaultConfig) error {
	if v := cfg.AccessRestriction; v != "" && !validAccessRestrictions[v] {
		return cerrors.Newf(cerrors.InvalidArgument, "invalid accessRestriction %q", v)
	}

	return nil
}

// defaultAccessRestriction applies the documented default: an absent or
// UNSPECIFIED access restriction becomes WITHIN_ORGANIZATION.
func defaultAccessRestriction(v string) string {
	if v == "" || v == accessUnspecified {
		return accessWithinOrganization
	}

	return v
}

// normalizeMask validates a required updateMask and returns the set of
// top-level camelCase fields it names. Paths may be snake_case (the proto
// spelling some clients send) or camelCase; a nested path
// (encryptionConfig.kmsKeyName) names its top-level field. An empty mask, an
// output-only field, or an unknown field is INVALID_ARGUMENT.
func normalizeMask(mask []string) (map[string]bool, error) {
	if len(mask) == 0 {
		return nil, cerrors.New(cerrors.InvalidArgument, "updateMask is required")
	}

	out := make(map[string]bool, len(mask))

	for _, path := range mask {
		top := snakeToCamel(strings.SplitN(path, ".", 2)[0]) //nolint:mnd // split into head and rest

		switch {
		case outputOnlyFields[top]:
			return nil, cerrors.Newf(cerrors.InvalidArgument, "updateMask path %q names an output-only field", path)
		case !mutableFields[top]:
			return nil, cerrors.Newf(cerrors.InvalidArgument, "updateMask path %q is not a BackupVault field", path)
		}

		out[top] = true
	}

	return out, nil
}

// applyMask copies each masked field from cfg onto v, validating the new value.
// Fields outside the mask are left untouched.
func applyMask(v *bdrdriver.BackupVault, cfg *bdrdriver.BackupVaultConfig, fields map[string]bool) error {
	checks := map[string]func(*bdrdriver.BackupVaultConfig) error{
		fieldRetention:     checkRetention,
		fieldInheritance:   checkInheritance,
		fieldEffectiveTime: checkEffectiveTime,
		fieldAccess:        checkAccessRestriction,
	}

	for f := range fields {
		if check, ok := checks[f]; ok {
			if err := check(cfg); err != nil {
				return err
			}
		}
	}

	setters := map[string]func(){
		fieldDescription:   func() { v.Description = cfg.Description },
		fieldLabels:        func() { v.Labels = cloneStrMap(cfg.Labels) },
		fieldAnnotations:   func() { v.Annotations = cloneStrMap(cfg.Annotations) },
		fieldRetention:     func() { v.BackupMinimumEnforcedRetentionDuration = cfg.BackupMinimumEnforcedRetentionDuration },
		fieldInheritance:   func() { v.BackupRetentionInheritance = cfg.BackupRetentionInheritance },
		fieldEffectiveTime: func() { v.EffectiveTime = cfg.EffectiveTime },
		fieldAccess:        func() { v.AccessRestriction = defaultAccessRestriction(cfg.AccessRestriction) },
		fieldEncryptionCfg: func() { v.EncryptionConfig = cloneEncryption(cfg.EncryptionConfig) },
	}

	for f := range fields {
		setters[f]()
	}

	return nil
}

// snakeToCamel converts a snake_case proto field name to its JSON camelCase
// spelling; a name without underscores is returned unchanged.
func snakeToCamel(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}

	parts := strings.Split(s, "_")

	var b strings.Builder

	b.WriteString(parts[0])

	for _, p := range parts[1:] {
		if p == "" {
			continue
		}

		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}

	return b.String()
}

// projectNumber derives a stable 12-digit project number from a project id, so
// the synthesized service account is deterministic.
func projectNumber(project string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(project))

	return projectNumberBase + h.Sum64()%projectNumberSpan
}

// serviceAccount returns the deterministic Backup and DR service agent for a
// project (see serviceAccountDomain).
func serviceAccount(project string) string {
	return "service-" + strconv.FormatUint(projectNumber(project), decimalBase) + serviceAccountDomain
}

// etagFor derives the vault etag from its identity, revision and update time,
// so it changes on every successful update and is deterministic under a fake
// clock.
func etagFor(v *bdrdriver.BackupVault) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(resourceName(v.Project, v.Location, v.ID) + etagRevisionSeparator +
		strconv.FormatInt(v.Revision, decimalBase) + etagRevisionSeparator + v.UpdateTime.Format(time.RFC3339Nano)))

	return strconv.FormatUint(h.Sum64(), hexBase)
}
