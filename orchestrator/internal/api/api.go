// Package api implements the orchestrator REST endpoints (SPEC section 6.1).
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hpo-grid/orchestrator/internal/mesh"
	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/provision"
	"github.com/hpo-grid/orchestrator/internal/queue"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
	"github.com/hpo-grid/orchestrator/internal/state"
)

// Server bundles the dependencies of the REST handlers.
type Server struct {
	Store       *state.Store
	Router      *queue.Router
	Mesh        *mesh.Client
	Provisioner provision.Provisioner
	ShardSize   int
}

// NewServer builds a Server with the given shard-size default.
func NewServer(store *state.Store, router *queue.Router, mc *mesh.Client, prov provision.Provisioner, shardSize int) *Server {
	if shardSize <= 0 {
		shardSize = 10
	}
	return &Server{Store: store, Router: router, Mesh: mc, Provisioner: prov, ShardSize: shardSize}
}

// Handler returns the root HTTP handler with all routes mounted.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /api/v1/jobs", s.createJob)
	mux.HandleFunc("GET /api/v1/jobs", s.listJobs)
	mux.HandleFunc("GET /api/v1/jobs/{jobId}", s.getJob)
	mux.HandleFunc("GET /api/v1/jobs/{jobId}/results", s.getJobResults)
	mux.HandleFunc("GET /api/v1/nodes", s.getNodes)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response failed: %v", err)
	}
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// validateJob checks the job submission payload.
func validateJob(j *model.Job) error {
	if j.Type != "hpo" && j.Type != "batch" {
		return fmt.Errorf("type must be \"hpo\" or \"batch\", got %q", j.Type)
	}
	if j.Payload.Objective == "" {
		return errors.New("payload.objective is required")
	}
	if j.Type == "batch" && len(j.Payload.Data) == 0 {
		return errors.New("payload.data must be non-empty for batch jobs")
	}
	if j.Type == "hpo" && len(j.Payload.SearchSpace) == 0 {
		return errors.New("payload.searchSpace must be non-empty for hpo jobs")
	}
	for name, b := range j.Payload.SearchSpace {
		if b[0] > b[1] {
			return fmt.Errorf("payload.searchSpace.%s: low %v > high %v", name, b[0], b[1])
		}
	}
	return nil
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var job model.Job
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&job); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid job JSON: "+err.Error())
		return
	}
	if err := validateJob(&job); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if job.JobID == "" {
		job.JobID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	} else if existing, err := s.Store.GetJob(job.JobID); err == nil && existing != nil {
		// client-supplied IDs must not silently overwrite an existing job
		writeErr(w, http.StatusConflict, "job "+job.JobID+" already exists")
		return
	}
	if job.ShardSize <= 0 {
		job.ShardSize = s.ShardSize
	}
	if job.StrategyID == "" {
		job.StrategyID = "gen-0"
	}
	job.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	job.Status = "running"

	// Seed initial tasks; for hpo, sample around the current strategy if one exists.
	var strat *model.Strategy
	if job.Type == "hpo" {
		st, err := s.Store.GetStrategy(job.JobID)
		if err != nil {
			log.Printf("api: loading strategy for %s failed: %v", job.JobID, err)
		}
		strat = st
	}
	tasks := queue.ShardJob(&job, strat)

	if err := s.Store.SaveJob(&job); err != nil {
		writeErr(w, http.StatusBadGateway, "storing job failed: "+err.Error())
		return
	}
	if err := s.Router.Enqueue(tasks); err != nil {
		writeErr(w, http.StatusBadGateway, "enqueueing tasks failed: "+err.Error())
		return
	}
	if s.Mesh != nil {
		s.Mesh.PublishJobSubmitted(&job)
	}
	if s.Provisioner != nil {
		if err := s.Provisioner.EnsureWorkers(r.Context(), job.JobID, 0); err != nil {
			log.Printf("api: provisioning workers for %s failed: %v", job.JobID, err)
		}
	}
	writeJSON(w, http.StatusCreated, &job)
}

type jobDetail struct {
	Job             model.Job      `json:"job"`
	Counters        model.Counters `json:"counters"`
	Best            *model.Result  `json:"best,omitempty"`
	StaleProcessing int            `json:"staleProcessing"`
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")
	job, err := s.Store.GetJob(jobID)
	if err == redisclient.ErrNil {
		writeErr(w, http.StatusNotFound, "job not found: "+jobID)
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadGateway, "loading job failed: "+err.Error())
		return
	}
	counters, err := s.Store.Counters(jobID)
	if err != nil {
		log.Printf("api: counters for %s failed: %v", jobID, err)
	}
	best, err := s.Store.Best(jobID)
	if err != nil {
		log.Printf("api: best for %s failed: %v", jobID, err)
	}
	stale, err := s.Router.StaleProcessing(jobID, 5*time.Minute)
	if err != nil {
		log.Printf("api: stale processing check for %s failed: %v", jobID, err)
	}
	writeJSON(w, http.StatusOK, &jobDetail{
		Job:             *job,
		Counters:        counters,
		Best:            best,
		StaleProcessing: len(stale),
	})
}

func (s *Server) getJobResults(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")
	if _, err := s.Store.GetJob(jobID); err == redisclient.ErrNil {
		writeErr(w, http.StatusNotFound, "job not found: "+jobID)
		return
	}
	results, err := s.Store.Results(jobID)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "loading results failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"jobId": jobID, "results": results})
}

func (s *Server) listJobs(w http.ResponseWriter, _ *http.Request) {
	jobs, err := s.Store.ListJobs()
	if err != nil {
		writeErr(w, http.StatusBadGateway, "listing jobs failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"jobs": jobs})
}

func (s *Server) getNodes(w http.ResponseWriter, _ *http.Request) {
	nodes, err := s.Store.Nodes()
	if err != nil {
		writeErr(w, http.StatusBadGateway, "loading nodes failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"nodes": nodes})
}
