package managedkafka

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
	mkdriver "github.com/stackshy/cloudemu/v2/services/managedkafka/driver"
)

const (
	clusterTypeURL = "type.googleapis.com/google.cloud.managedkafka.v1.Cluster"
	emptyTypeURL   = "type.googleapis.com/google.protobuf.Empty"

	int64Base = 10
	int64Bits = 64
)

// int64String is a proto3-JSON int64: marshaled as a decimal string, and
// accepted on input as either a string or a bare number (both are legal proto3
// JSON). The Go discovery client tags these fields `json:",string"`.
type int64String int64

// MarshalJSON renders the value as a JSON string.
func (v int64String) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(v), int64Base))
}

// UnmarshalJSON accepts "123" or 123.
func (v *int64String) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)

	n, err := strconv.ParseInt(s, int64Base, int64Bits)
	if err != nil {
		return err
	}

	*v = int64String(n)

	return nil
}

type capacityJSON struct {
	VcpuCount   int64String `json:"vcpuCount,omitempty"`
	MemoryBytes int64String `json:"memoryBytes,omitempty"`
}

type networkConfigJSON struct {
	Subnet string `json:"subnet,omitempty"`
}

type accessConfigJSON struct {
	NetworkConfigs []networkConfigJSON `json:"networkConfigs,omitempty"`
}

type gcpConfigJSON struct {
	AccessConfig *accessConfigJSON `json:"accessConfig,omitempty"`
	KmsKey       string            `json:"kmsKey,omitempty"`
}

type rebalanceJSON struct {
	Mode string `json:"mode,omitempty"`
}

// clusterJSON mirrors the managedkafka v1 Cluster message. Output-only fields
// (name, state, createTime, updateTime, satisfiesPzi/Pzs) are ignored on input.
type clusterJSON struct {
	Name            string            `json:"name,omitempty"`
	CapacityConfig  *capacityJSON     `json:"capacityConfig,omitempty"`
	GcpConfig       *gcpConfigJSON    `json:"gcpConfig,omitempty"`
	RebalanceConfig *rebalanceJSON    `json:"rebalanceConfig,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	State           string            `json:"state,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	SatisfiesPzi    bool              `json:"satisfiesPzi,omitempty"`
	SatisfiesPzs    bool              `json:"satisfiesPzs,omitempty"`
}

// topicJSON mirrors the managedkafka v1 Topic message (int32 counts are plain
// JSON numbers).
type topicJSON struct {
	Name              string            `json:"name,omitempty"`
	PartitionCount    int32             `json:"partitionCount,omitempty"`
	ReplicationFactor int32             `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// operationJSON mirrors google.longrunning.Operation. Mutating ops complete
// inline, so `done` is always true.
type operationJSON struct {
	Name     string          `json:"name"`
	Done     bool            `json:"done"`
	Response json.RawMessage `json:"response,omitempty"`
}

// toDriverCluster converts a request body into a driver cluster scoped to rt.
func toDriverCluster(in *clusterJSON, rt *route, id string) *mkdriver.Cluster {
	c := &mkdriver.Cluster{
		Project:  rt.project,
		Location: rt.location,
		ID:       id,
		Labels:   in.Labels,
	}

	if in.CapacityConfig != nil {
		c.VcpuCount = int64(in.CapacityConfig.VcpuCount)
		c.MemoryBytes = int64(in.CapacityConfig.MemoryBytes)
	}

	if in.GcpConfig != nil {
		c.KmsKey = in.GcpConfig.KmsKey

		if in.GcpConfig.AccessConfig != nil {
			for _, nc := range in.GcpConfig.AccessConfig.NetworkConfigs {
				c.Subnets = append(c.Subnets, nc.Subnet)
			}
		}
	}

	if in.RebalanceConfig != nil {
		c.RebalanceMode = in.RebalanceConfig.Mode
	}

	return c
}

// fromDriverCluster renders a driver cluster as managedkafka v1 wire JSON.
func fromDriverCluster(c *mkdriver.Cluster) clusterJSON {
	out := clusterJSON{
		Name: clusterName(c.Project, c.Location, c.ID),
		CapacityConfig: &capacityJSON{
			VcpuCount:   int64String(c.VcpuCount),
			MemoryBytes: int64String(c.MemoryBytes),
		},
		GcpConfig:    &gcpConfigJSON{KmsKey: c.KmsKey, AccessConfig: &accessConfigJSON{}},
		Labels:       c.Labels,
		State:        c.State,
		CreateTime:   formatTime(c.CreateTime),
		UpdateTime:   formatTime(c.UpdateTime),
		SatisfiesPzi: c.SatisfiesPzi,
		SatisfiesPzs: c.SatisfiesPzs,
	}

	for _, s := range c.Subnets {
		out.GcpConfig.AccessConfig.NetworkConfigs = append(out.GcpConfig.AccessConfig.NetworkConfigs,
			networkConfigJSON{Subnet: s})
	}

	if c.RebalanceMode != "" {
		out.RebalanceConfig = &rebalanceJSON{Mode: c.RebalanceMode}
	}

	return out
}

// toDriverTopic converts a request body into a driver topic scoped to rt.
func toDriverTopic(in *topicJSON, rt *route, id string) *mkdriver.Topic {
	return &mkdriver.Topic{
		Project:           rt.project,
		Location:          rt.location,
		ClusterID:         rt.cluster,
		ID:                id,
		PartitionCount:    in.PartitionCount,
		ReplicationFactor: in.ReplicationFactor,
		Configs:           in.Configs,
	}
}

// fromDriverTopic renders a driver topic as managedkafka v1 wire JSON.
func fromDriverTopic(t *mkdriver.Topic) topicJSON {
	return topicJSON{
		Name:              topicName(t.Project, t.Location, t.ClusterID, t.ID),
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
		Configs:           t.Configs,
	}
}

// decodeBody decodes a JSON request body into v; an empty body leaves v zero.
// A malformed body is 400 INVALID_ARGUMENT.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, gcprest.MaxBodyBytes)

	if err := json.NewDecoder(r.Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
		gcprest.WriteError(w, http.StatusBadRequest, "invalid", "malformed JSON body: "+err.Error())
		return false
	}

	return true
}

// anyWithType marshals v as a google.protobuf.Any by adding the "@type"
// discriminator to its JSON object.
func anyWithType(v any, typeURL string) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var fields map[string]json.RawMessage
	if uErr := json.Unmarshal(raw, &fields); uErr != nil {
		return nil, uErr
	}

	if fields == nil {
		fields = map[string]json.RawMessage{}
	}

	typ, err := json.Marshal(typeURL)
	if err != nil {
		return nil, err
	}

	fields["@type"] = typ

	return json.Marshal(fields)
}

// writeOperation writes a completed operation whose response is v (typed as
// typeURL) and records it with the shared LRO poller (a no-op on a nil
// registry), so a client polling the returned name resolves the same done
// operation with its response.
func (h *Handler) writeOperation(w http.ResponseWriter, op *mkdriver.Operation, v any, typeURL string) {
	resp, err := anyWithType(v, typeURL)
	if err != nil {
		gcprest.WriteError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	if h.ops != nil {
		h.ops.Register(op.Name, resp)
	}

	gcprest.WriteJSON(w, http.StatusOK, operationJSON{Name: op.Name, Done: true, Response: resp})
}

// clusterName builds the full cluster resource name.
func clusterName(project, location, id string) string {
	return "projects/" + project + "/locations/" + location + "/" + clustersSeg + "/" + id
}

// topicName builds the full topic resource name.
func topicName(project, location, clusterID, id string) string {
	return clusterName(project, location, clusterID) + "/" + topicsSeg + "/" + id
}

// formatTime renders t as RFC3339Nano UTC; a zero time renders as "".
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339Nano)
}
