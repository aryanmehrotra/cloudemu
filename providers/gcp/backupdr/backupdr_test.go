package backupdr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stackshy/cloudemu/v2/config"
	cerrors "github.com/stackshy/cloudemu/v2/errors"
	bdrdriver "github.com/stackshy/cloudemu/v2/services/backupdr/driver"
)

const (
	testProject  = "p"
	testLocation = "us-central1"
	testVault    = "vault-1"
)

func newMock(t *testing.T) (*Mock, *config.FakeClock) {
	t.Helper()

	clk := config.NewFakeClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))

	return New(config.NewOptions(config.WithProjectID(testProject), config.WithClock(clk))), clk
}

func vaultCfg(id string) *bdrdriver.BackupVaultConfig {
	return &bdrdriver.BackupVaultConfig{
		Project: testProject, Location: testLocation, ID: id,
		Description:                            "d",
		Labels:                                 map[string]string{"env": "dev"},
		BackupMinimumEnforcedRetentionDuration: "86400s",
	}
}

func TestCreateMintsOutputFields(t *testing.T) {
	m, clk := newMock(t)
	ctx := context.Background()

	v, op, err := m.CreateBackupVault(ctx, vaultCfg(testVault))
	if err != nil {
		t.Fatalf("CreateBackupVault: %v", err)
	}

	if !op.Done || !strings.HasPrefix(op.Name, "projects/p/locations/us-central1/operations/") {
		t.Fatalf("operation = %+v", op)
	}

	if v.State != stateActive || v.AccessRestriction != accessWithinOrganization || !v.Deletable() {
		t.Fatalf("defaults not applied: %+v", v)
	}

	if v.ServiceAccount != serviceAccount(testProject) || !strings.HasSuffix(v.ServiceAccount, serviceAccountDomain) {
		t.Fatalf("serviceAccount = %q", v.ServiceAccount)
	}

	if v.UID == "" || v.Etag == "" || !v.CreateTime.Equal(clk.Now()) {
		t.Fatalf("uid/etag/createTime not minted: %+v", v)
	}

	// Returned values never alias the store.
	v.Labels["env"] = "mutated"

	got, err := m.GetBackupVault(ctx, testProject, testLocation, testVault)
	if err != nil {
		t.Fatalf("GetBackupVault: %v", err)
	}

	if got.Labels["env"] != "dev" {
		t.Fatalf("store aliased by returned value: %v", got.Labels)
	}

	if _, _, err := m.CreateBackupVault(ctx, vaultCfg(testVault)); !cerrors.IsAlreadyExists(err) {
		t.Fatalf("duplicate create err = %v, want AlreadyExists", err)
	}
}

func TestCreateValidation(t *testing.T) {
	m, _ := newMock(t)
	ctx := context.Background()

	cases := map[string]func(*bdrdriver.BackupVaultConfig){
		"missing retention":   func(c *bdrdriver.BackupVaultConfig) { c.BackupMinimumEnforcedRetentionDuration = "" },
		"malformed retention": func(c *bdrdriver.BackupVaultConfig) { c.BackupMinimumEnforcedRetentionDuration = "1d" },
		"negative retention":  func(c *bdrdriver.BackupVaultConfig) { c.BackupMinimumEnforcedRetentionDuration = "-5s" },
		"short id":            func(c *bdrdriver.BackupVaultConfig) { c.ID = "ab" },
		"bad access":          func(c *bdrdriver.BackupVaultConfig) { c.AccessRestriction = "NOPE" },
		"bad inheritance":     func(c *bdrdriver.BackupVaultConfig) { c.BackupRetentionInheritance = "NOPE" },
		"bad effectiveTime":   func(c *bdrdriver.BackupVaultConfig) { c.EffectiveTime = "yesterday" },
		"wildcard location":   func(c *bdrdriver.BackupVaultConfig) { c.Location = anyLocation },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := vaultCfg(testVault)
			mutate(cfg)

			if _, _, err := m.CreateBackupVault(ctx, cfg); !cerrors.IsInvalidArgument(err) {
				t.Fatalf("err = %v, want InvalidArgument", err)
			}
		})
	}

	for _, ok := range []string{"0s", "1.5s", "3600s"} {
		cfg := vaultCfg("ok-" + strings.ReplaceAll(strings.TrimSuffix(ok, "s"), ".", "-"))
		cfg.BackupMinimumEnforcedRetentionDuration = ok

		if _, _, err := m.CreateBackupVault(ctx, cfg); err != nil {
			t.Fatalf("retention %q rejected: %v", ok, err)
		}
	}
}

func TestValidateOnlyDoesNotPersist(t *testing.T) {
	m, _ := newMock(t)
	ctx := context.Background()

	cfg := vaultCfg(testVault)
	cfg.ValidateOnly = true

	if _, _, err := m.CreateBackupVault(ctx, cfg); err != nil {
		t.Fatalf("validateOnly create: %v", err)
	}

	if _, err := m.GetBackupVault(ctx, testProject, testLocation, testVault); !cerrors.IsNotFound(err) {
		t.Fatalf("validateOnly create persisted the vault: %v", err)
	}
}

func TestUpdateMaskAndEtag(t *testing.T) {
	m, clk := newMock(t)
	ctx := context.Background()

	created, _, err := m.CreateBackupVault(ctx, vaultCfg(testVault))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	clk.Advance(time.Minute)

	patch := &bdrdriver.BackupVaultConfig{
		Project: testProject, Location: testLocation, ID: testVault,
		Description: "new", Labels: map[string]string{"x": "y"}, Etag: created.Etag,
	}

	updated, _, err := m.UpdateBackupVault(ctx, patch, []string{"description"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if updated.Description != "new" || updated.Labels["env"] != "dev" {
		t.Fatalf("mask not honored: %+v", updated)
	}

	if updated.Etag == created.Etag || !updated.UpdateTime.After(created.UpdateTime) {
		t.Fatalf("etag/updateTime not rotated: %q -> %q", created.Etag, updated.Etag)
	}

	// Stale etag (the create-time one) is rejected.
	if _, _, err := m.UpdateBackupVault(ctx, patch, []string{"labels"}); !errors.Is(err, bdrdriver.ErrEtagMismatch) {
		t.Fatalf("stale etag err = %v, want ErrEtagMismatch", err)
	}

	// snake_case mask paths are accepted.
	patch.Etag = ""
	patch.BackupMinimumEnforcedRetentionDuration = "172800s"

	got, _, err := m.UpdateBackupVault(ctx, patch, []string{"backup_minimum_enforced_retention_duration"})
	if err != nil || got.BackupMinimumEnforcedRetentionDuration != "172800s" {
		t.Fatalf("snake_case mask: %v %+v", err, got)
	}

	for _, bad := range [][]string{nil, {"state"}, {"etag"}, {"bogus"}} {
		if _, _, err := m.UpdateBackupVault(ctx, patch, bad); !cerrors.IsInvalidArgument(err) {
			t.Fatalf("mask %v err = %v, want InvalidArgument", bad, err)
		}
	}

	patch.BackupMinimumEnforcedRetentionDuration = "-1s"
	if _, _, err := m.UpdateBackupVault(ctx, patch, []string{fieldRetention}); !cerrors.IsInvalidArgument(err) {
		t.Fatalf("negative retention patch err = %v", err)
	}
}

func TestDeleteSemantics(t *testing.T) {
	m, _ := newMock(t)
	ctx := context.Background()

	v, _, err := m.CreateBackupVault(ctx, vaultCfg(testVault))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	req := &bdrdriver.DeleteBackupVaultRequest{Project: testProject, Location: testLocation, ID: testVault}

	if err := m.SetUsage(testProject, testLocation, testVault, 3, 1024); err != nil {
		t.Fatalf("SetUsage: %v", err)
	}

	if _, err := m.DeleteBackupVault(ctx, req); !cerrors.IsFailedPrecondition(err) || errors.Is(err, bdrdriver.ErrEtagMismatch) {
		t.Fatalf("non-empty delete err = %v, want FailedPrecondition", err)
	}

	req.Etag = "stale"
	if _, err := m.DeleteBackupVault(ctx, req); !errors.Is(err, bdrdriver.ErrEtagMismatch) {
		t.Fatalf("stale etag delete err = %v", err)
	}

	req.Etag = v.Etag
	req.Force = true

	if _, err := m.DeleteBackupVault(ctx, req); err != nil {
		t.Fatalf("force delete: %v", err)
	}

	if _, err := m.DeleteBackupVault(ctx, req); !cerrors.IsNotFound(err) {
		t.Fatalf("second delete err = %v, want NotFound", err)
	}

	req.AllowMissing = true

	if op, err := m.DeleteBackupVault(ctx, req); err != nil || !op.Done {
		t.Fatalf("allowMissing delete: %v %+v", err, op)
	}
}

func TestListScopesAndWildcard(t *testing.T) {
	m, _ := newMock(t)
	ctx := context.Background()

	for _, loc := range []string{"us-central1", "europe-west1"} {
		cfg := vaultCfg("vault-" + loc)
		cfg.Location = loc

		if _, _, err := m.CreateBackupVault(ctx, cfg); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	one, _ := m.ListBackupVaults(ctx, testProject, "us-central1")
	all, _ := m.ListBackupVaults(ctx, testProject, anyLocation)
	other, _ := m.ListBackupVaults(ctx, "other", anyLocation)

	if len(one) != 1 || len(all) != 2 || len(other) != 0 {
		t.Fatalf("list sizes = %d/%d/%d, want 1/2/0", len(one), len(all), len(other))
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	m, _ := newMock(t)
	ctx := context.Background()

	v, _, err := m.CreateBackupVault(ctx, vaultCfg(testVault))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	data, err := m.Snapshot(ctx, false)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored, _ := newMock(t)
	if err := restored.Restore(ctx, data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err := restored.GetBackupVault(ctx, testProject, testLocation, testVault)
	if err != nil || got.UID != v.UID || got.Etag != v.Etag {
		t.Fatalf("restored vault = %+v (err %v), want uid/etag of %+v", got, err, v)
	}

	_, op, err := restored.CreateBackupVault(ctx, vaultCfg("vault-2"))
	if err != nil || !strings.Contains(op.Name, "/operation-2-") {
		t.Fatalf("opSeq not restored: %v %+v", err, op)
	}
}
