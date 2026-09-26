// Package managedkafka provides an in-memory mock of the Google Cloud Managed
// Service for Apache Kafka control plane (managedkafka.googleapis.com/v1). It
// models clusters, the topics nested under them, and the long-running
// operations cluster mutations return. It is control-plane only: there are no
// brokers and no produce/consume data plane.
package managedkafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/stackshy/cloudemu/v2/config"
	cerrors "github.com/stackshy/cloudemu/v2/errors"
	"github.com/stackshy/cloudemu/v2/internal/idgen"
	"github.com/stackshy/cloudemu/v2/internal/memstore"
	mkdriver "github.com/stackshy/cloudemu/v2/services/managedkafka/driver"
)

var _ mkdriver.ManagedKafka = (*Mock)(nil)

const (
	clustersColl = "clusters"
	topicsColl   = "topics"

	// stateActive is the steady state a created cluster reports. Real Managed
	// Kafka passes through CREATING first; CloudEmu completes synchronously.
	stateActive = "ACTIVE"

	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

// Mock is the in-memory Managed Kafka control-plane implementation. Clusters and
// topics are keyed by their full GCP resource names.
type Mock struct {
	mu sync.RWMutex

	clusters   *memstore.Store[mkdriver.Cluster]
	topics     *memstore.Store[mkdriver.Topic]
	operations *memstore.Store[mkdriver.Operation]

	opSeq atomic.Uint64
	opts  *config.Options
}

// New creates a new Managed Kafka mock.
func New(opts *config.Options) *Mock {
	return &Mock{
		clusters:   memstore.New[mkdriver.Cluster](),
		topics:     memstore.New[mkdriver.Topic](),
		operations: memstore.New[mkdriver.Operation](),
		opts:       opts,
	}
}

// clusterName builds the full cluster resource name.
func clusterName(project, location, id string) string {
	return "projects/" + project + "/locations/" + location + "/" + clustersColl + "/" + id
}

// topicName builds the full topic resource name.
func topicName(project, location, clusterID, id string) string {
	return clusterName(project, location, clusterID) + "/" + topicsColl + "/" + id
}

// newOp records a completed operation scoped to the project+location it acted in
// and returns it. The caller holds the write lock.
func (m *Mock) newOp(project, location, opType, target string) *mkdriver.Operation {
	scope := "projects/" + project + "/locations/" + location
	op := mkdriver.Operation{
		Name:       fmt.Sprintf("%s/operations/operation-%d-%s", scope, m.opSeq.Add(1), idgen.UUID()),
		Done:       true,
		TargetName: target,
		Type:       opType,
	}
	m.operations.Set(op.Name, op)

	return &op
}

// CreateCluster validates and stores a new cluster, reporting it ACTIVE, and
// returns the completed LRO.
func (m *Mock) CreateCluster(_ context.Context, c *mkdriver.Cluster) (*mkdriver.Cluster, *mkdriver.Operation, error) {
	if err := validateClusterID(c.ID); err != nil {
		return nil, nil, err
	}

	if err := validateCluster(c); err != nil {
		return nil, nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := clusterName(c.Project, c.Location, c.ID)
	if m.clusters.Has(key) {
		return nil, nil, cerrors.Newf(cerrors.AlreadyExists, "cluster %q already exists", key)
	}

	now := m.opts.Clock.Now().UTC()
	stored := cloneCluster(c)
	stored.State = stateActive
	stored.CreateTime = now
	stored.UpdateTime = now
	m.clusters.Set(key, stored)

	op := m.newOp(c.Project, c.Location, opCreate, key)
	out := cloneCluster(&stored)

	return &out, op, nil
}

// GetCluster returns a cluster by identity, cloned.
func (m *Mock) GetCluster(_ context.Context, project, location, id string) (*mkdriver.Cluster, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.clusters.Get(clusterName(project, location, id))
	if !ok {
		return nil, clusterNotFound(project, location, id)
	}

	out := cloneCluster(&c)

	return &out, nil
}

// ListClusters returns every cluster in a project+location, ordered by name.
func (m *Mock) ListClusters(_ context.Context, project, location string) ([]mkdriver.Cluster, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := "projects/" + project + "/locations/" + location + "/" + clustersColl + "/"
	all := m.clusters.SortedValues()
	out := make([]mkdriver.Cluster, 0, len(all))

	for i := range all {
		if strings.HasPrefix(clusterName(all[i].Project, all[i].Location, all[i].ID), prefix) {
			out = append(out, cloneCluster(&all[i]))
		}
	}

	return out, nil
}

// UpdateCluster applies the masked fields of c to the stored cluster,
// re-validates the result, and returns the completed LRO. Unknown, immutable and
// output-only mask paths are rejected with INVALID_ARGUMENT before anything
// changes.
func (m *Mock) UpdateCluster(_ context.Context, c *mkdriver.Cluster, mask []string) (
	*mkdriver.Cluster, *mkdriver.Operation, error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := clusterName(c.Project, c.Location, c.ID)

	stored, ok := m.clusters.Get(key)
	if !ok {
		return nil, nil, clusterNotFound(c.Project, c.Location, c.ID)
	}

	next := cloneCluster(&stored)
	if err := applyClusterMask(&next, c, mask); err != nil {
		return nil, nil, err
	}

	if err := validateCluster(&next); err != nil {
		return nil, nil, err
	}

	next.UpdateTime = m.opts.Clock.Now().UTC()
	m.clusters.Set(key, next)

	op := m.newOp(c.Project, c.Location, opUpdate, key)
	out := cloneCluster(&next)

	return &out, op, nil
}

// DeleteCluster removes a cluster together with every topic under it and returns
// the completed LRO.
func (m *Mock) DeleteCluster(_ context.Context, project, location, id string) (*mkdriver.Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := clusterName(project, location, id)
	if !m.clusters.Has(key) {
		return nil, clusterNotFound(project, location, id)
	}

	m.clusters.Delete(key)

	prefix := key + "/" + topicsColl + "/"
	for _, k := range m.topics.Keys() {
		if strings.HasPrefix(k, prefix) {
			m.topics.Delete(k)
		}
	}

	return m.newOp(project, location, opDelete, key), nil
}

// GetOperation returns a (done) long-running operation by name. An unknown name
// is reported as a done operation: the mock completes synchronously, so any op
// id a standalone poll asks for has already finished. In an assembled server
// the shared LRO poller answers polls instead, and 404s unknown names.
func (m *Mock) GetOperation(_ context.Context, name string) (*mkdriver.Operation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	op, ok := m.operations.Get(name)
	if !ok {
		return &mkdriver.Operation{Name: name, Done: true}, nil
	}

	return &op, nil
}

// clusterNotFound builds the NOT_FOUND error carrying the full resource name.
func clusterNotFound(project, location, id string) error {
	return cerrors.Newf(cerrors.NotFound, "cluster %q not found", clusterName(project, location, id))
}
