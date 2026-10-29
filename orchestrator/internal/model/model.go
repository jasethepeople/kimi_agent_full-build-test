// Package model holds the shared message schemas (SPEC section 4).
// JSON tags must match the spec exactly.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// Job is the job registry record (POST body / grid:job value).
type Job struct {
	JobID      string     `json:"jobId"`
	Type       string     `json:"type"` // hpo|batch
	Payload    JobPayload `json:"payload"`
	ShardSize  int        `json:"shardSize"`
	MaxWorkers int        `json:"maxWorkers"`
	Status     string     `json:"status"` // pending|running|completed|failed
	CreatedAt  string     `json:"createdAt"`
	StrategyID string     `json:"strategyId"`
}

// JobPayload carries the workload definition.
type JobPayload struct {
	Objective   string                `json:"objective"`
	SearchSpace map[string][2]float64 `json:"searchSpace,omitempty"`
	Data        []json.RawMessage     `json:"data,omitempty"`
}

// Task is a queued work chunk.
type Task struct {
	TaskID     string             `json:"taskId"`
	JobID      string             `json:"jobId"`
	ShardIndex int                `json:"shardIndex"`
	Params     map[string]float64 `json:"params,omitempty"`
	Items      []json.RawMessage  `json:"items,omitempty"`
	StrategyID string             `json:"strategyId"`
	EnqueuedAt string             `json:"enqueuedAt"`
}

// Result is a completed task outcome.
type Result struct {
	TaskID      string           `json:"taskId"`
	JobID       string           `json:"jobId"`
	WorkerID    string           `json:"workerId"`
	Status      string           `json:"status"` // ok|failed|duplicate|deadend
	Score       float64          `json:"score"`
	Metrics     map[string]int64 `json:"metrics"`
	Output      string           `json:"output"`
	StrategyID  string           `json:"strategyId"`
	CompletedAt string           `json:"completedAt"`
}

// Strategy is the active parameter strategy.
type Strategy struct {
	StrategyID     string             `json:"strategyId"`
	JobID          string             `json:"jobId"`
	Generation     int                `json:"generation"`
	Params         map[string]float64 `json:"params"`
	MutationReason string             `json:"mutationReason"`
	CreatedAt      string             `json:"createdAt"`
}

// Node is a grid:state registry entry.
type Node struct {
	NodeID    string            `json:"nodeId"`
	Role      string            `json:"role"` // orchestrator|strategy-engine|worker
	StartedAt string            `json:"startedAt"`
	LastBeat  string            `json:"lastBeat"`
	Meta      map[string]string `json:"meta"`
}

// Counters is the grid:counters:{jobId} hash.
type Counters struct {
	Submitted int64 `json:"submitted"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Deadends  int64 `json:"deadends"`
}

// ParamHash computes the dedup hash: sha256 hex of canonical JSON of the
// params map (keys sorted, compact separators).
func ParamHash(params map[string]float64) string {
	if params == nil {
		params = map[string]float64{}
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf []byte
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(params[k])
		buf = append(buf, kb...)
		buf = append(buf, ':')
		buf = append(buf, vb...)
	}
	buf = append(buf, '}')
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// CanonicalJSON renders params as canonical JSON (keys sorted, compact).
func CanonicalJSON(params map[string]float64) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := "{"
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(params[k])
		out += string(kb) + ":" + string(vb)
	}
	return out + "}"
}
