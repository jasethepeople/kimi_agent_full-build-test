package queue

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
)

func batchJob(n int, shardSize int) *model.Job {
	data := make([]json.RawMessage, n)
	for i := range data {
		data[i] = json.RawMessage(fmt.Sprintf(`"item-%d"`, i))
	}
	return &model.Job{
		JobID:     "job-t",
		Type:      "batch",
		ShardSize: shardSize,
		Payload:   model.JobPayload{Objective: "hash", Data: data},
	}
}

func TestShardBatch(t *testing.T) {
	job := batchJob(25, 10)
	tasks := ShardJob(job, nil)
	if len(tasks) != 3 {
		t.Fatalf("expected 3 shards for 25 items/size 10, got %d", len(tasks))
	}
	sizes := []int{10, 10, 5}
	total := 0
	for i, task := range tasks {
		if len(task.Items) != sizes[i] {
			t.Fatalf("shard %d has %d items, want %d", i, len(task.Items), sizes[i])
		}
		if task.ShardIndex != i {
			t.Fatalf("shard %d has ShardIndex %d", i, task.ShardIndex)
		}
		if task.JobID != job.JobID || task.TaskID == "" || task.EnqueuedAt == "" {
			t.Fatalf("shard %d missing fields: %+v", i, task)
		}
		total += len(task.Items)
	}
	if total != 25 {
		t.Fatalf("total items = %d, want 25", total)
	}
}

func TestShardBatchExactMultiple(t *testing.T) {
	tasks := ShardJob(batchJob(20, 10), nil)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(tasks))
	}
}

func TestShardHPO(t *testing.T) {
	job := &model.Job{
		JobID:     "job-hpo",
		Type:      "hpo",
		ShardSize: 8,
		Payload: model.JobPayload{
			Objective:   "minimize",
			SearchSpace: map[string][2]float64{"lr": {0.0001, 0.1}, "depth": {1, 8}},
		},
	}
	tasks := ShardJob(job, nil)
	if len(tasks) != 8 {
		t.Fatalf("expected 8 hpo candidate tasks, got %d", len(tasks))
	}
	for _, task := range tasks {
		if len(task.Params) != 2 {
			t.Fatalf("task params = %v, want 2 entries", task.Params)
		}
		for name, bounds := range job.Payload.SearchSpace {
			v := task.Params[name]
			if v < bounds[0] || v > bounds[1] {
				t.Fatalf("param %s=%v out of [%v,%v]", name, v, bounds[0], bounds[1])
			}
		}
	}
}

func TestSampleAroundClamps(t *testing.T) {
	space := map[string][2]float64{"x": {0, 1}}
	center := map[string]float64{"x": 0.99}
	for i := 0; i < 200; i++ {
		p := SampleAround(center, space)
		if p["x"] < 0 || p["x"] > 1 {
			t.Fatalf("sample %v out of bounds", p["x"])
		}
	}
	// center above range must clamp
	p := SampleAround(map[string]float64{"x": 5}, space)
	if p["x"] < 0 || p["x"] > 1 {
		t.Fatalf("clamped sample %v out of bounds", p["x"])
	}
}

func TestEnqueueAndStaleProcessing(t *testing.T) {
	fake := redisclient.NewFake()
	r := NewRouter(fake)
	job := batchJob(5, 2)
	tasks := ShardJob(job, nil)
	if err := r.Enqueue(tasks); err != nil {
		t.Fatal(err)
	}
	vals, err := fake.LRange("grid:queue:job-t", 0, -1)
	if err != nil || len(vals) != len(tasks) {
		t.Fatalf("queue len = %d err=%v, want %d", len(vals), err, len(tasks))
	}
	m, err := fake.HGetAll("grid:counters:job-t")
	if err != nil || m["submitted"] != "3" {
		t.Fatalf("submitted counter = %q err=%v, want 3", m["submitted"], err)
	}

	// stale processing: old entry reported, fresh one not
	old := model.Task{TaskID: "t-old", JobID: "job-t", EnqueuedAt: time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)}
	fresh := model.Task{TaskID: "t-fresh", JobID: "job-t", EnqueuedAt: time.Now().UTC().Format(time.RFC3339)}
	ob, _ := json.Marshal(old)
	fb, _ := json.Marshal(fresh)
	fake.HSet("grid:processing:job-t", "t-old", string(ob), "t-fresh", string(fb))
	stale, err := r.StaleProcessing("job-t", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].TaskID != "t-old" {
		t.Fatalf("stale = %+v, want only t-old", stale)
	}
}
