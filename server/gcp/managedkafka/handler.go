// Package managedkafka implements the Google Cloud Managed Service for Apache
// Kafka control plane (managedkafka.googleapis.com/v1) as a server.Handler. Real
// google.golang.org/api/managedkafka/v1 clients and the Terraform google
// provider's google_managed_kafka_cluster / google_managed_kafka_topic resources
// hit this handler unchanged.
//
// Coverage:
//
//	POST   /v1/…/clusters?clusterId=               : CreateCluster (LRO)
//	GET    /v1/…/clusters                          : ListClusters
//	GET    /v1/…/clusters/{c}                      : GetCluster
//	PATCH  /v1/…/clusters/{c}?updateMask=          : UpdateCluster (LRO)
//	DELETE /v1/…/clusters/{c}                      : DeleteCluster (LRO)
//	POST   /v1/…/clusters/{c}/topics?topicId=      : CreateTopic (sync, returns Topic)
//	GET    /v1/…/clusters/{c}/topics[/{t}]         : List/GetTopic
//	PATCH  /v1/…/clusters/{c}/topics/{t}?updateMask= : UpdateTopic (sync, returns Topic)
//	DELETE /v1/…/clusters/{c}/topics/{t}           : DeleteTopic (sync, returns Empty)
//	GET    /v1/…/operations/{op}                   : Operations.Get (shared poller)
//
// Path sharing: /v1/projects/{p}/locations/{l}/clusters[/{c}] is byte-identical
// to GKE's (container/v1) and AlloyDB's cluster paths, and a custom-endpoint
// client sends the emulator's own Host, so URL alone cannot tell them apart. In
// an assembled server (a shared LRO registry is wired) this handler registers
// AHEAD of GKE/AlloyDB and claims a cluster request only when it is genuinely
// Managed Kafka traffic, the Filestore/Spanner content+ownership pattern:
//
//   - a create whose body carries capacityConfig or gcpConfig (GKE wraps its
//     body in {"cluster": …}; AlloyDB bodies carry neither key);
//   - an item request for a cluster this store owns;
//   - a list in a project+location where this store owns at least one cluster;
//   - anything under clusters/{c}/topics (no sibling service has topics).
//
// Everything else falls through to GKE/AlloyDB. Operation polls are yielded to
// the shared LRO poller. A standalone package server (no registry) claims every
// clusters/topics/operations path.
package managedkafka

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/stackshy/cloudemu/v2/server/gcp/lro"
	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
	mkdriver "github.com/stackshy/cloudemu/v2/services/managedkafka/driver"
)

const (
	pathPrefix    = "/v1/projects/"
	projectsSeg   = "projects"
	locationsSeg  = "locations"
	operationsSeg = "operations"
	clustersSeg   = "clusters"
	topicsSeg     = "topics"

	scopeParts = 4 // [projects, {p}, locations, {l}]

	// rest-segment counts after the location scope.
	restCollection = 1 // [clusters]
	restItem       = 2 // [clusters, {c}]
	restTopics     = 3 // [clusters, {c}, topics]
	restTopic      = 4 // [clusters, {c}, topics, {t}]

	maxProbeBytes = 1 << 20
)

// Handler serves managedkafka.googleapis.com v1 requests against a ManagedKafka
// driver.
type Handler struct {
	db mkdriver.ManagedKafka

	// ops records created operations with the shared poller. Nil in a standalone
	// package server, where this handler serves its own /operations/ poll and
	// claims every clusters path.
	ops *lro.Registry
}

// New returns a Managed Kafka handler backed by db.
func New(db mkdriver.ManagedKafka) *Handler { return &Handler{db: db} }

// SetOperationRegistry wires the shared LRO poller so created operations are
// resolvable (with their response) through the full server's operations route,
// and switches Matches to the content+ownership mode an assembled server needs.
func (h *Handler) SetOperationRegistry(reg *lro.Registry) { h.ops = reg }

// route holds the parsed components of a Managed Kafka v1 path.
type route struct {
	project  string
	location string
	resource string // clusters | operations
	cluster  string // cluster id, or operation id for an operations route
	topics   bool   // path is under clusters/{c}/topics
	topic    string // topic id; empty for the topics collection
}

// parseRoute extracts the components of a Managed Kafka v1 path. It recognizes
// only the clusters (with nested topics) and operations resources under a
// locations scope; a custom verb (clusters/{c}:promote, …) is not ours.
func parseRoute(urlPath string) (route, bool) {
	if !strings.HasPrefix(urlPath, pathPrefix) || strings.Contains(urlPath, ":") {
		return route{}, false
	}

	parts := strings.Split(strings.TrimPrefix(urlPath, "/v1/"), "/")
	if len(parts) <= scopeParts || parts[0] != projectsSeg || parts[2] != locationsSeg || slices.Contains(parts, "") {
		return route{}, false
	}

	rt := route{project: parts[1], location: parts[3]}
	if !rt.setRest(parts[scopeParts:]) {
		return route{}, false
	}

	return rt, true
}

// setRest fills the resource components from the segments after the location
// scope: operations[/{op}] or clusters[/{c}[/topics[/{t}]]].
func (rt *route) setRest(rest []string) bool {
	switch {
	case rest[0] == operationsSeg && len(rest) <= restItem:
	case rest[0] == clustersSeg && len(rest) < restTopics:
	case rest[0] == clustersSeg && len(rest) <= restTopic && rest[2] == topicsSeg:
		rt.topics = true
	default:
		return false
	}

	rt.resource = rest[0]

	if len(rest) >= restItem {
		rt.cluster = rest[1]
	}

	if len(rest) == restTopic {
		rt.topic = rest[3]
	}

	return true
}

// Matches claims Managed Kafka paths. See the package doc for how it shares the
// clusters path with GKE and AlloyDB in an assembled server.
func (h *Handler) Matches(r *http.Request) bool {
	rt, ok := parseRoute(r.URL.Path)
	if !ok {
		return false
	}

	standalone := h.ops == nil

	switch {
	case rt.resource == operationsSeg:
		return standalone
	case standalone || rt.topics:
		return true
	case rt.cluster != "":
		_, err := h.db.GetCluster(r.Context(), rt.project, rt.location, rt.cluster)

		return err == nil
	case r.Method == http.MethodPost:
		return bodyLooksLikeKafka(r)
	case r.Method == http.MethodGet:
		all, err := h.db.ListClusters(r.Context(), rt.project, rt.location)

		return err == nil && len(all) > 0
	default:
		return false
	}
}

// bodyLooksLikeKafka reports whether a POST .../clusters body is a Managed Kafka
// Cluster (it carries capacityConfig or gcpConfig) rather than a GKE
// CreateClusterRequest or an AlloyDB Cluster. It reads and restores the body so
// a fall-through handler still sees the full request.
func bodyLooksLikeKafka(r *http.Request) bool {
	if r.Body == nil {
		return false
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxProbeBytes))
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(raw))

	if err != nil {
		return false
	}

	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) != nil {
		return false
	}

	_, hasCapacity := probe["capacityConfig"]
	_, hasGcp := probe["gcpConfig"]

	return hasCapacity || hasGcp
}

// ServeHTTP routes on the parsed path and method.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rt, ok := parseRoute(r.URL.Path)
	if !ok {
		gcprest.WriteError(w, http.StatusNotFound, "notFound", "unrecognized Managed Kafka path")
		return
	}

	switch {
	case rt.resource == operationsSeg:
		h.serveOperation(w, r)
	case rt.topics && rt.topic == "":
		h.serveTopicCollection(w, r, &rt)
	case rt.topics:
		h.serveTopicItem(w, r, &rt)
	case rt.cluster == "":
		h.serveClusterCollection(w, r, &rt)
	default:
		h.serveClusterItem(w, r, &rt)
	}
}

func (h *Handler) serveClusterCollection(w http.ResponseWriter, r *http.Request, rt *route) {
	switch r.Method {
	case http.MethodPost:
		h.createCluster(w, r, rt)
	case http.MethodGet:
		h.listClusters(w, r, rt)
	default:
		writeMethodNotAllowed(w)
	}
}

func (h *Handler) serveClusterItem(w http.ResponseWriter, r *http.Request, rt *route) {
	switch r.Method {
	case http.MethodGet:
		h.getCluster(w, r, rt)
	case http.MethodPatch:
		h.updateCluster(w, r, rt)
	case http.MethodDelete:
		h.deleteCluster(w, r, rt)
	default:
		writeMethodNotAllowed(w)
	}
}

func (h *Handler) serveTopicCollection(w http.ResponseWriter, r *http.Request, rt *route) {
	switch r.Method {
	case http.MethodPost:
		h.createTopic(w, r, rt)
	case http.MethodGet:
		h.listTopics(w, r, rt)
	default:
		writeMethodNotAllowed(w)
	}
}

func (h *Handler) serveTopicItem(w http.ResponseWriter, r *http.Request, rt *route) {
	switch r.Method {
	case http.MethodGet:
		h.getTopic(w, r, rt)
	case http.MethodPatch:
		h.updateTopic(w, r, rt)
	case http.MethodDelete:
		h.deleteTopic(w, r, rt)
	default:
		writeMethodNotAllowed(w)
	}
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	gcprest.WriteError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "method not allowed")
}
