// Package state manages the Redis state graph, job registry and counters.
package state

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
)

// Redis keys (SPEC section 3).
const (
	KeyJobs     = "grid:jobs"
	KeyJob      = "grid:job:%s"
	KeyResults  = "grid:results:%s"
	KeyBest     = "grid:best:%s"
	KeyStrategy = "grid:strategy:%s"
	KeyState    = "grid:state"
	KeyCounters = "grid:counters:%s"
)

// Store wraps the Redis client with job/state-graph operations.
type Store struct {
	R      redisclient.Client
	Node   model.Node
	stopHB context.CancelFunc
}

// New creates a Store and registers the node record.
func New(r redisclient.Client, nodeID, role string, meta map[string]string) *Store {
	now := time.Now().UTC().Format(time.RFC3339)
	return &Store{
		R: r,
		Node: model.Node{
			NodeID:    nodeID,
			Role:      role,
			StartedAt: now,
			LastBeat:  now,
			Meta:      meta,
		},
	}
}

// Heartbeat writes the node record to grid:state once.
func (s *Store) Heartbeat() error {
	s.Node.LastBeat = time.Now().UTC().Format(time.RFC3339)
	b, err := json.Marshal(s.Node)
	if err != nil {
		return err
	}
	_, err = s.R.HSet(KeyState, s.Node.NodeID, string(b))
	return err
}

// StartHeartbeat heartbeats every interval until ctx is cancelled.
func (s *Store) StartHeartbeat(ctx context.Context, interval time.Duration) {
	if err := s.Heartbeat(); err != nil {
		log.Printf("state: initial heartbeat failed: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	s.stopHB = cancel
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.Heartbeat(); err != nil {
					log.Printf("state: heartbeat failed: %v", err)
				}
			}
		}
	}()
}

// SaveJob writes a job to the registry (hash + standalone string key).
func (s *Store) SaveJob(j *model.Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if _, err := s.R.HSet(KeyJobs, j.JobID, string(b)); err != nil {
		return err
	}
	return s.R.Set(fmt.Sprintf(KeyJob, j.JobID), string(b))
}

// GetJob loads a job; redisclient.ErrNil if unknown.
func (s *Store) GetJob(jobID string) (*model.Job, error) {
	raw, err := s.R.Get(fmt.Sprintf(KeyJob, jobID))
	if err != nil {
		return nil, err
	}
	var j model.Job
	if err := json.Unmarshal([]byte(raw), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// ListJobs returns all jobs in the registry.
func (s *Store) ListJobs() ([]model.Job, error) {
	m, err := s.R.HGetAll(KeyJobs)
	if err != nil {
		return nil, err
	}
	jobs := make([]model.Job, 0, len(m))
	for _, raw := range m {
		var j model.Job
		if err := json.Unmarshal([]byte(raw), &j); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// Counters returns the progress counters for a job.
func (s *Store) Counters(jobID string) (model.Counters, error) {
	m, err := s.R.HGetAll(fmt.Sprintf(KeyCounters, jobID))
	if err != nil {
		return model.Counters{}, err
	}
	var c model.Counters
	get := func(k string) int64 {
		n, _ := strconv.ParseInt(m[k], 10, 64)
		return n
	}
	c.Submitted = get("submitted")
	c.Completed = get("completed")
	c.Failed = get("failed")
	c.Deadends = get("deadends")
	return c, nil
}

// Best returns the best-so-far result for a job, or nil when none.
func (s *Store) Best(jobID string) (*model.Result, error) {
	raw, err := s.R.Get(fmt.Sprintf(KeyBest, jobID))
	if err == redisclient.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r model.Result
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Results lists all results for a job.
func (s *Store) Results(jobID string) ([]model.Result, error) {
	vals, err := s.R.LRange(fmt.Sprintf(KeyResults, jobID), 0, -1)
	if err != nil {
		return nil, err
	}
	out := make([]model.Result, 0, len(vals))
	for _, raw := range vals {
		var r model.Result
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// SaveStrategy writes the active strategy for a job.
func (s *Store) SaveStrategy(st *model.Strategy) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.R.Set(fmt.Sprintf(KeyStrategy, st.JobID), string(b))
}

// GetStrategy loads the active strategy for a job, or nil when none.
func (s *Store) GetStrategy(jobID string) (*model.Strategy, error) {
	raw, err := s.R.Get(fmt.Sprintf(KeyStrategy, jobID))
	if err == redisclient.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st model.Strategy
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// Nodes returns the full state graph snapshot.
func (s *Store) Nodes() ([]model.Node, error) {
	m, err := s.R.HGetAll(KeyState)
	if err != nil {
		return nil, err
	}
	out := make([]model.Node, 0, len(m))
	for _, raw := range m {
		var n model.Node
		if err := json.Unmarshal([]byte(raw), &n); err != nil {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}
