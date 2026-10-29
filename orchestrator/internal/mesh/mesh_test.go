package mesh

import (
	"encoding/json"
	"testing"

	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/queue"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
	"github.com/hpo-grid/orchestrator/internal/state"
)

type fakePublisher struct{ msgs map[string][][]byte }

func (f *fakePublisher) Publish(subject string, payload []byte) error {
	f.msgs[subject] = append(f.msgs[subject], payload)
	return nil
}

func newFixture(t *testing.T) (*Client, *redisclient.FakeClient) {
	t.Helper()
	fake := redisclient.NewFake()
	store := state.New(fake, "node-mesh", "orchestrator", nil)
	router := queue.NewRouter(fake)
	return New(&fakePublisher{msgs: map[string][][]byte{}}, store, router), fake
}

func seedHPOJob(t *testing.T, c *Client) *model.Job {
	t.Helper()
	job := &model.Job{
		JobID:      "job-s",
		Type:       "hpo",
		ShardSize:  5,
		Status:     "running",
		StrategyID: "gen-0",
		Payload: model.JobPayload{
			Objective:   "minimize",
			SearchSpace: map[string][2]float64{"lr": {0.0001, 0.1}, "wd": {0, 0.01}},
		},
	}
	if err := c.store.SaveJob(job); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestApplyStrategyUpdateGeneratesCandidates(t *testing.T) {
	c, fake := newFixture(t)
	seedHPOJob(t, c)

	st := &model.Strategy{
		StrategyID:     "gen-3",
		JobID:          "job-s",
		Generation:     3,
		Params:         map[string]float64{"lr": 0.01, "wd": 0.001},
		MutationReason: "stagnation",
	}
	n, err := c.ApplyStrategyUpdate(st)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("enqueued %d tasks, want shardSize=5", n)
	}

	// strategy persisted to grid:strategy:{jobId}
	raw, err := fake.Get("grid:strategy:job-s")
	if err != nil {
		t.Fatalf("strategy not stored: %v", err)
	}
	var stored model.Strategy
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.StrategyID != "gen-3" || stored.Generation != 3 {
		t.Fatalf("stored strategy = %+v", stored)
	}

	// new candidate tasks in the queue carry the new strategyId and in-bounds params
	vals, err := fake.LRange("grid:queue:job-s", 0, -1)
	if err != nil || len(vals) != 5 {
		t.Fatalf("queue len = %d err=%v", len(vals), err)
	}
	for _, rawTask := range vals {
		var task model.Task
		if err := json.Unmarshal([]byte(rawTask), &task); err != nil {
			t.Fatal(err)
		}
		if task.StrategyID != "gen-3" {
			t.Fatalf("task strategyId = %q, want gen-3", task.StrategyID)
		}
		if task.Params["lr"] < 0.0001 || task.Params["lr"] > 0.1 {
			t.Fatalf("lr=%v out of bounds", task.Params["lr"])
		}
		if task.Params["wd"] < 0 || task.Params["wd"] > 0.01 {
			t.Fatalf("wd=%v out of bounds", task.Params["wd"])
		}
	}

	// submitted counter bumped
	m, _ := fake.HGetAll("grid:counters:job-s")
	if m["submitted"] != "5" {
		t.Fatalf("submitted = %q, want 5", m["submitted"])
	}
}

func TestApplyStrategyUpdateSkipsBatch(t *testing.T) {
	c, fake := newFixture(t)
	job := &model.Job{
		JobID:     "job-b",
		Type:      "batch",
		Status:    "running",
		ShardSize: 3,
		Payload:   model.JobPayload{Objective: "hash"},
	}
	if err := c.store.SaveJob(job); err != nil {
		t.Fatal(err)
	}
	st := &model.Strategy{StrategyID: "gen-1", JobID: "job-b", Generation: 1, Params: map[string]float64{"x": 1}}
	n, err := c.ApplyStrategyUpdate(st)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("batch job should get 0 new tasks, got %d", n)
	}
	// strategy still persisted
	if _, err := fake.Get("grid:strategy:job-b"); err != nil {
		t.Fatalf("strategy not stored: %v", err)
	}
}

func TestApplyStrategyUpdateUnknownJob(t *testing.T) {
	c, fake := newFixture(t)
	st := &model.Strategy{StrategyID: "gen-9", JobID: "job-unknown", Generation: 9, Params: map[string]float64{"x": 1}}
	n, err := c.ApplyStrategyUpdate(st)
	if err != nil || n != 0 {
		t.Fatalf("unknown job: n=%d err=%v", n, err)
	}
	if _, err := fake.Get("grid:strategy:job-unknown"); err != nil {
		t.Fatalf("strategy not stored: %v", err)
	}
}

func TestPublishJobSubmitted(t *testing.T) {
	c, _ := newFixture(t)
	job := &model.Job{JobID: "job-p", Type: "hpo", Status: "running"}
	c.PublishJobSubmitted(job)
	pub := c.Pub.(*fakePublisher)
	msgs := pub.msgs["grid.jobs.submitted"]
	if len(msgs) != 1 {
		t.Fatalf("published %d msgs, want 1", len(msgs))
	}
	var got model.Job
	if err := json.Unmarshal(msgs[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.JobID != "job-p" {
		t.Fatalf("published job = %+v", got)
	}
}

func TestNilPublisherDegrades(t *testing.T) {
	c, _ := newFixture(t)
	c.Pub = nil
	// must not panic
	c.PublishJobSubmitted(&model.Job{JobID: "j"})
	c.PublishHello(&model.Node{NodeID: "n"})
}
