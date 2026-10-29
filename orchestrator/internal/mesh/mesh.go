// Package mesh wires the orchestrator to NATS: publishing events and applying
// strategy updates broadcast by the strategy engine.
package mesh

import (
	"encoding/json"
	"log"
	"time"

	"github.com/hpo-grid/orchestrator/internal/model"
	"github.com/hpo-grid/orchestrator/internal/natsclient"
	"github.com/hpo-grid/orchestrator/internal/queue"
	"github.com/hpo-grid/orchestrator/internal/state"
)

// NATS subjects (SPEC section 5).
const (
	SubjNodesHello      = "grid.nodes.hello"
	SubjJobsSubmitted   = "grid.jobs.submitted"
	SubjStrategyUpdate  = "grid.strategy.update.%s"
	SubjStrategyUpdates = "grid.strategy.update.*"
)

// Publisher is the publish half of a NATS connection (or a test fake).
type Publisher interface {
	Publish(subject string, payload []byte) error
}

// Client is a mesh connection. It may be nil when NATS is unavailable; all
// operations then degrade gracefully to logged no-ops.
type Client struct {
	Pub    Publisher
	store  *state.Store
	router *queue.Router
}

// New builds a Client. pub may be nil (NATS down).
func New(pub Publisher, store *state.Store, router *queue.Router) *Client {
	return &Client{Pub: pub, store: store, router: router}
}

func (c *Client) publish(subject string, v interface{}) {
	if c.Pub == nil {
		log.Printf("mesh: NATS unavailable, dropping publish to %s", subject)
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("mesh: marshal for %s failed: %v", subject, err)
		return
	}
	if err := c.Pub.Publish(subject, b); err != nil {
		log.Printf("mesh: publish to %s failed: %v", subject, err)
	}
}

// PublishJobSubmitted announces a new job.
func (c *Client) PublishJobSubmitted(j *model.Job) {
	c.publish(SubjJobsSubmitted, j)
}

// PublishHello announces this node at startup.
func (c *Client) PublishHello(n *model.Node) {
	c.publish(SubjNodesHello, n)
}

// ApplyStrategyUpdate stores the new active strategy and, for HPO jobs,
// generates and enqueues fresh candidate tasks derived from the mutated
// strategy params. Returns the number of new tasks enqueued.
func (c *Client) ApplyStrategyUpdate(st *model.Strategy) (int, error) {
	if err := c.store.SaveStrategy(st); err != nil {
		return 0, err
	}
	job, err := c.store.GetJob(st.JobID)
	if err != nil {
		// unknown job; strategy stored anyway
		return 0, nil
	}
	if job.Type != "hpo" || job.Status == "completed" || job.Status == "failed" {
		return 0, nil
	}
	job.StrategyID = st.StrategyID
	tasks := queue.ShardJob(job, st)
	if err := c.router.Enqueue(tasks); err != nil {
		return 0, err
	}
	log.Printf("mesh: applied strategy %s (gen %d) for job %s; enqueued %d new candidate tasks",
		st.StrategyID, st.Generation, st.JobID, len(tasks))
	return len(tasks), nil
}

// SubscribeStrategyUpdates wires grid.strategy.update.* into ApplyStrategyUpdate.
func SubscribeStrategyUpdates(conn *natsclient.Conn, c *Client) error {
	if conn == nil {
		log.Printf("mesh: NATS unavailable, strategy updates will not be received")
		return nil
	}
	return conn.Subscribe(SubjStrategyUpdates, func(m *natsclient.Message) {
		var st model.Strategy
		if err := json.Unmarshal(m.Data, &st); err != nil {
			log.Printf("mesh: bad strategy update on %s: %v", m.Subject, err)
			return
		}
		if st.CreatedAt == "" {
			st.CreatedAt = time.Now().UTC().Format(time.RFC3339)
		}
		if _, err := c.ApplyStrategyUpdate(&st); err != nil {
			log.Printf("mesh: applying strategy update for job %s failed: %v", st.JobID, err)
		}
	})
}
