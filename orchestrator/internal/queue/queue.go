// Package queue shards jobs into tasks and routes them onto Redis lists.
package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
)

const (
	// KeyQueue is the pending task list for a job.
	KeyQueue = "grid:queue:%s"
	// KeyProcessing is the in-flight task hash for a job.
	KeyProcessing = "grid:processing:%s"
	// KeyCounters is the per-job counters hash.
	KeyCounters = "grid:counters:%s"
)

// Router shards jobs and LPUSHes tasks to per-job queues.
type Router struct {
	R redisclient.Client
}

// NewRouter builds a Router.
func NewRouter(r redisclient.Client) *Router {
	return &Router{R: r}
}

func queueKey(jobID string) string { return fmt.Sprintf(KeyQueue, jobID) }

// randHex returns n hex characters from crypto/rand.
func randHex(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		// extremely unlikely; fall back to time-based uniqueness
		return fmt.Sprintf("%x", time.Now().UnixNano())[:n]
	}
	return hex.EncodeToString(b)[:n]
}

// randFloat returns a uniform float64 in [0,1).
func randFloat() float64 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<53))
	if err != nil {
		return float64(time.Now().UnixNano()%1e9) / 1e9
	}
	return float64(n.Int64()) / float64(1<<53)
}

// SampleUniform draws one candidate point uniformly from the search space.
func SampleUniform(space map[string][2]float64) map[string]float64 {
	params := make(map[string]float64, len(space))
	for name, bounds := range space {
		lo, hi := bounds[0], bounds[1]
		params[name] = lo + randFloat()*(hi-lo)
	}
	return params
}

// SampleAround draws a candidate point as a gaussian-ish perturbation around a
// center strategy's params, clamped to the search space. Params absent from
// the center fall back to uniform sampling.
func SampleAround(center map[string]float64, space map[string][2]float64) map[string]float64 {
	params := make(map[string]float64, len(space))
	for name, bounds := range space {
		lo, hi := bounds[0], bounds[1]
		span := hi - lo
		v, ok := center[name]
		if !ok {
			params[name] = lo + randFloat()*span
			continue
		}
		// perturbation in [-0.2, 0.2] of span (sum of two uniforms approximates gaussian)
		delta := (randFloat() + randFloat() - 1.0) * 0.2 * span
		v += delta
		if v < lo {
			v = lo
		}
		if v > hi {
			v = hi
		}
		params[name] = v
	}
	return params
}

// ShardJob converts a job into tasks and returns them (does not enqueue).
//   - batch: payload.data is chunked into groups of shardSize items.
//   - hpo: shardSize initial candidate points are sampled from the search
//     space (around the given strategy's params when provided).
func ShardJob(job *model.Job, strategy *model.Strategy) []model.Task {
	now := time.Now().UTC().Format(time.RFC3339)
	size := job.ShardSize
	if size <= 0 {
		size = 10
	}
	var tasks []model.Task
	if job.Type == "batch" {
		for i := 0; i < len(job.Payload.Data); i += size {
			end := i + size
			if end > len(job.Payload.Data) {
				end = len(job.Payload.Data)
			}
			items := make([]json.RawMessage, end-i)
			copy(items, job.Payload.Data[i:end])
			idx := len(tasks)
			tasks = append(tasks, model.Task{
				TaskID:     fmt.Sprintf("task-%s-%d-%s", job.JobID, idx, randHex(6)),
				JobID:      job.JobID,
				ShardIndex: idx,
				Items:      items,
				StrategyID: job.StrategyID,
				EnqueuedAt: now,
			})
		}
		return tasks
	}
	// hpo
	for i := 0; i < size; i++ {
		var params map[string]float64
		if strategy != nil && len(strategy.Params) > 0 {
			params = SampleAround(strategy.Params, job.Payload.SearchSpace)
		} else {
			params = SampleUniform(job.Payload.SearchSpace)
		}
		tasks = append(tasks, model.Task{
			TaskID:     fmt.Sprintf("task-%s-%d-%s", job.JobID, i, randHex(6)),
			JobID:      job.JobID,
			ShardIndex: i,
			Params:     params,
			StrategyID: job.StrategyID,
			EnqueuedAt: now,
		})
	}
	return tasks
}

// Enqueue LPUSHes tasks onto the job queue and bumps the submitted counter.
func (r *Router) Enqueue(tasks []model.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	jobID := tasks[0].JobID
	vals := make([]string, 0, len(tasks))
	for _, t := range tasks {
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		vals = append(vals, string(b))
	}
	if _, err := r.R.LPush(queueKey(jobID), vals...); err != nil {
		return err
	}
	_, err := r.R.HIncrBy(fmt.Sprintf(KeyCounters, jobID), "submitted", int64(len(tasks)))
	return err
}

// StaleProcessing returns entries in grid:processing:{jobId} whose task was
// enqueued (and presumably picked up) more than maxAge ago. Report-only.
func (r *Router) StaleProcessing(jobID string, maxAge time.Duration) ([]model.Task, error) {
	m, err := r.R.HGetAll(fmt.Sprintf(KeyProcessing, jobID))
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-maxAge)
	stale := []model.Task{}
	for _, raw := range m {
		var t model.Task
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			continue
		}
		ts, err := time.Parse(time.RFC3339, t.EnqueuedAt)
		if err != nil || ts.Before(cutoff) {
			stale = append(stale, t)
		}
	}
	return stale, nil
}
