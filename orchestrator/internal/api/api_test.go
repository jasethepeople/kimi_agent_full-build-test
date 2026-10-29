package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hpo-grid/orchestrator/internal/mesh"
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

func newTestServer(t *testing.T) (*Server, *redisclient.FakeClient, *fakePublisher) {
	t.Helper()
	fake := redisclient.NewFake()
	store := state.New(fake, "node-test", "orchestrator", map[string]string{})
	router := queue.NewRouter(fake)
	pub := &fakePublisher{msgs: map[string][][]byte{}}
	mc := mesh.New(pub, store, router)
	srv := NewServer(store, router, mc, nil, 10)
	return srv, fake, pub
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	srv, _, _ := newTestServer(t)
	rec := do(t, srv.Handler(), "GET", "/healthz", "")
	if rec.Code != 200 {
		t.Fatalf("healthz = %d", rec.Code)
	}
	var body map[string]string
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Fatalf("healthz body = %v", body)
	}
}

func TestCreateBatchJob(t *testing.T) {
	srv, fake, pub := newTestServer(t)
	h := srv.Handler()
	body := `{"type":"batch","payload":{"objective":"hash","data":["a","b","c","d","e"]},"shardSize":2,"maxWorkers":2}`
	rec := do(t, h, "POST", "/api/v1/jobs", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/v1/jobs = %d: %s", rec.Code, rec.Body)
	}
	var job model.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.JobID == "" || job.Status != "running" || job.StrategyID != "gen-0" || job.CreatedAt == "" {
		t.Fatalf("job fields not populated: %+v", job)
	}
	// tasks enqueued: 5 items / size 2 -> 3 tasks
	vals, _ := fake.LRange("grid:queue:"+job.JobID, 0, -1)
	if len(vals) != 3 {
		t.Fatalf("queued %d tasks, want 3", len(vals))
	}
	// job stored in both registry locations
	if _, err := fake.Get("grid:job:" + job.JobID); err != nil {
		t.Fatalf("grid:job missing: %v", err)
	}
	m, _ := fake.HGetAll("grid:jobs")
	if len(m) != 1 {
		t.Fatalf("grid:jobs = %v", m)
	}
	// counters + mesh publish
	c, _ := fake.HGetAll("grid:counters:" + job.JobID)
	if c["submitted"] != "3" {
		t.Fatalf("submitted = %q", c["submitted"])
	}
	if len(pub.msgs["grid.jobs.submitted"]) != 1 {
		t.Fatalf("grid.jobs.submitted not published: %v", pub.msgs)
	}

	// GET job detail
	rec = do(t, h, "GET", "/api/v1/jobs/"+job.JobID, "")
	if rec.Code != 200 {
		t.Fatalf("GET job = %d", rec.Code)
	}
	var detail struct {
		Job      model.Job      `json:"job"`
		Counters model.Counters `json:"counters"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Job.JobID != job.JobID || detail.Counters.Submitted != 3 {
		t.Fatalf("detail = %+v", detail)
	}

	// results (empty) and list
	rec = do(t, h, "GET", "/api/v1/jobs/"+job.JobID+"/results", "")
	if rec.Code != 200 {
		t.Fatalf("GET results = %d", rec.Code)
	}
	rec = do(t, h, "GET", "/api/v1/jobs", "")
	if rec.Code != 200 {
		t.Fatalf("GET jobs = %d", rec.Code)
	}
	var list struct {
		Jobs []model.Job `json:"jobs"`
	}
	json.Unmarshal(rec.Body.Bytes(), &list)
	if len(list.Jobs) != 1 {
		t.Fatalf("list jobs = %+v", list.Jobs)
	}
}

func TestCreateHPOJob(t *testing.T) {
	srv, fake, _ := newTestServer(t)
	h := srv.Handler()
	body := `{"jobId":"job-hpo-1","type":"hpo","payload":{"objective":"minimize","searchSpace":{"lr":[0.0001,0.1]}},"shardSize":4}`
	rec := do(t, h, "POST", "/api/v1/jobs", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST hpo = %d: %s", rec.Code, rec.Body)
	}
	vals, _ := fake.LRange("grid:queue:job-hpo-1", 0, -1)
	if len(vals) != 4 {
		t.Fatalf("queued %d hpo tasks, want 4", len(vals))
	}
	var task model.Task
	if err := json.Unmarshal([]byte(vals[0]), &task); err != nil {
		t.Fatal(err)
	}
	lr := task.Params["lr"]
	if lr < 0.0001 || lr > 0.1 {
		t.Fatalf("lr=%v out of search space", lr)
	}
}

func TestCreateJobValidation(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	cases := []string{
		`{"type":"weird","payload":{"objective":"x"}}`,                          // bad type
		`{"type":"batch","payload":{"objective":"x"}}`,                          // no data
		`{"type":"hpo","payload":{"objective":"x"}}`,                            // no search space
		`{"type":"batch","payload":{"data":["a"]}}`,                             // no objective
		`{"type":"hpo","payload":{"objective":"x","searchSpace":{"lr":[2,1]}}}`, // inverted bounds
		`not json`,
	}
	for i, body := range cases {
		rec := do(t, h, "POST", "/api/v1/jobs", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("case %d: status = %d, want 400 (%s)", i, rec.Code, body)
		}
	}
}

func TestGetJobNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	rec := do(t, h, "GET", "/api/v1/jobs/job-missing", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	rec = do(t, h, "GET", "/api/v1/jobs/job-missing/results", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("results status = %d, want 404", rec.Code)
	}
}

func TestGetNodes(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.Handler()
	if err := srv.Store.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	rec := do(t, h, "GET", "/api/v1/nodes", "")
	if rec.Code != 200 {
		t.Fatalf("GET nodes = %d", rec.Code)
	}
	var body struct {
		Nodes []model.Node `json:"nodes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Nodes) != 1 || body.Nodes[0].Role != "orchestrator" || body.Nodes[0].NodeID != "node-test" {
		t.Fatalf("nodes = %+v", body.Nodes)
	}
}

func TestCreateJobConflictOnDuplicateID(t *testing.T) {
	srv, _, _ := newTestServer(t)
	body := `{"jobId":"dup-1","type":"batch","payload":{"objective":"count","data":["a"]}}`
	resp1 := do(t, srv.Handler(), http.MethodPost, "/api/v1/jobs", body)
	if resp1.Code != http.StatusCreated {
		t.Fatalf("first POST = %d", resp1.Code)
	}
	resp2 := do(t, srv.Handler(), http.MethodPost, "/api/v1/jobs", body)
	if resp2.Code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d, want 409", resp2.Code)
	}
}
