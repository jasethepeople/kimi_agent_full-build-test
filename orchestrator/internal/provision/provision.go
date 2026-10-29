// Package provision manages the Docker worker lifecycle. It talks to the
// Docker daemon over the unix socket with a minimal stdlib HTTP client and
// degrades gracefully (no-op with warning) when the socket is unavailable.
package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Provisioner ensures a worker pool exists for a job.
type Provisioner interface {
	EnsureWorkers(ctx context.Context, jobID string, replicas int) error
}

// Config holds provisioner settings from the environment.
type Config struct {
	SocketPath  string // default /var/run/docker.sock
	WorkerImage string // WORKER_IMAGE
	Replicas    int    // WORKER_REPLICAS
	RedisURL    string // REDIS_URL
	NATSURL     string // NATS_URL
	Network     string // optional docker network (default: grid)
}

// New picks an implementation: a Docker client when the socket exists,
// otherwise a no-op provisioner.
func New(cfg Config) Provisioner {
	if cfg.SocketPath == "" {
		cfg.SocketPath = "/var/run/docker.sock"
	}
	if _, err := os.Stat(cfg.SocketPath); err != nil {
		log.Printf("provision: docker socket %s unavailable (%v); worker provisioning disabled (external workers still supported)", cfg.SocketPath, err)
		return &Noop{reason: "docker socket unavailable"}
	}
	return &Docker{cfg: cfg}
}

// Noop is a graceful-degradation provisioner.
type Noop struct{ reason string }

// EnsureWorkers logs and returns nil.
func (n *Noop) EnsureWorkers(_ context.Context, jobID string, _ int) error {
	log.Printf("provision: skipping worker provisioning for job %s: %s", jobID, n.reason)
	return nil
}

// Docker implements Provisioner via the Docker Engine API over a unix socket.
type Docker struct {
	cfg Config
}

func (d *Docker) httpClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", d.cfg.SocketPath)
			},
		},
	}
}

func (d *Docker) request(ctx context.Context, method, path string, body interface{}) ([]byte, int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, rdr)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.httpClient().Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return b, resp.StatusCode, err
}

type containerSummary struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
}

func (d *Docker) exists(ctx context.Context, name string) (bool, error) {
	b, status, err := d.request(ctx, "GET",
		fmt.Sprintf("/containers/json?all=1&filters=%%7B%%22name%%22:%%5B%%22%s%%22%%5D%%7D", name), nil)
	if err != nil {
		return false, err
	}
	if status >= 300 {
		return false, fmt.Errorf("docker: list containers: status %d: %s", status, b)
	}
	var cs []containerSummary
	if err := json.Unmarshal(b, &cs); err != nil {
		return false, err
	}
	for _, c := range cs {
		for _, n := range c.Names {
			if strings.TrimPrefix(n, "/") == name {
				return true, nil
			}
		}
	}
	return false, nil
}

// EnsureWorkers idempotently creates and starts replicas containers named
// hpo-worker-{jobId}-{i} for the job.
func (d *Docker) EnsureWorkers(ctx context.Context, jobID string, replicas int) error {
	if replicas <= 0 {
		replicas = d.cfg.Replicas
	}
	if replicas <= 0 {
		replicas = 2
	}
	image := d.cfg.WorkerImage
	if image == "" {
		image = "hpo-grid/worker:latest"
	}
	for i := 0; i < replicas; i++ {
		name := fmt.Sprintf("hpo-worker-%s-%d", jobID, i)
		ok, err := d.exists(ctx, name)
		if err != nil {
			log.Printf("provision: existence check for %s failed: %v (continuing)", name, err)
			continue
		}
		if ok {
			continue // idempotent
		}
		env := []string{
			"REDIS_URL=" + d.cfg.RedisURL,
			"NATS_URL=" + d.cfg.NATSURL,
			"JOB_ID=" + jobID,
		}
		spec := map[string]interface{}{
			"Image": image,
			"Env":   env,
			"HostConfig": map[string]interface{}{
				"NetworkMode": firstNonEmpty(d.cfg.Network, "grid"),
			},
		}
		b, status, err := d.request(ctx, "POST", "/containers/create?name="+name, spec)
		if err != nil || status >= 300 {
			log.Printf("provision: create %s failed: status=%d err=%v body=%s", name, status, err, b)
			continue
		}
		var created struct {
			ID string `json:"Id"`
		}
		if err := json.Unmarshal(b, &created); err != nil {
			log.Printf("provision: decode create response for %s failed: %v", name, err)
			continue
		}
		b, status, err = d.request(ctx, "POST", "/containers/"+created.ID+"/start", nil)
		if err != nil || status >= 300 {
			log.Printf("provision: start %s failed: status=%d err=%v body=%s", name, status, err, b)
			continue
		}
		log.Printf("provision: started worker container %s", name)
	}
	return nil
}

func firstNonEmpty(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
