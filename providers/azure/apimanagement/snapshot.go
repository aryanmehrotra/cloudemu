package apimanagement

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stackshy/cloudemu/v2/internal/snapshot"
)

var _ snapshot.Snapshottable = (*Mock)(nil)

// snapshotState is the on-disk shape: the service store keyed by its
// (lowercased) resource id.
type snapshotState struct {
	Services json.RawMessage `json:"services,omitempty"`
}

// Snapshot captures every API Management service. includeAssets is unused:
// these resources hold no bulk object bodies.
func (m *Mock) Snapshot(_ context.Context, _ bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	services, err := m.services.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("apimanagement: snapshot services: %w", err)
	}

	data, err := json.Marshal(snapshotState{Services: services})
	if err != nil {
		return nil, fmt.Errorf("apimanagement: marshal snapshot: %w", err)
	}

	return data, nil
}

// Restore rebuilds every service under its original id.
func (m *Mock) Restore(_ context.Context, data json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(data) == 0 {
		return nil
	}

	var state snapshotState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("apimanagement: unmarshal snapshot: %w", err)
	}

	if len(state.Services) == 0 {
		return nil
	}

	if err := m.services.LoadSnapshot(state.Services); err != nil {
		return fmt.Errorf("apimanagement: restore services: %w", err)
	}

	return nil
}
