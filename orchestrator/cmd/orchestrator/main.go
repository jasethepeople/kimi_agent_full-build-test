// Command orchestrator runs the HPO grid Go core orchestrator.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hpo-grid/orchestrator/internal/api"
	"github.com/hpo-grid/orchestrator/internal/mesh"
	"github.com/hpo-grid/orchestrator/internal/natsclient"
	"github.com/hpo-grid/orchestrator/internal/provision"
	"github.com/hpo-grid/orchestrator/internal/queue"
	"github.com/hpo-grid/orchestrator/internal/redisclient"
	"github.com/hpo-grid/orchestrator/internal/state"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func main() {
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	redisURL := env("REDIS_URL", "redis://localhost:6379")
	natsURL := env("NATS_URL", "nats://localhost:4222")
	apiAddr := env("API_ADDR", ":8080")
	gridID := env("GRID_ID", "grid-0")
	shardSize := envInt("JOB_SHARD_SIZE", 10)

	rdb := redisclient.New(redisAddr)
	defer rdb.Close()
	// wait for Redis with backoff (graceful degradation at startup)
	redisOK := false
	for i := 0; i < 10; i++ {
		err := rdb.Ping()
		if err == nil {
			redisOK = true
			break
		}
		wait := time.Duration(100*(1<<i)) * time.Millisecond
		if wait > 3*time.Second {
			wait = 3 * time.Second
		}
		log.Printf("orchestrator: redis at %s not ready (%v), retry in %s", redisAddr, err, wait)
		time.Sleep(wait)
	}
	if !redisOK {
		log.Printf("orchestrator: WARNING redis at %s unreachable; API will return errors until redis recovers", redisAddr)
	}

	nodeID := fmt.Sprintf("orchestrator-%s-%d", gridID, time.Now().UnixNano())
	store := state.New(rdb, nodeID, "orchestrator", map[string]string{"gridId": gridID})
	router := queue.NewRouter(rdb)

	// NATS (optional; degrade gracefully)
	var nc *natsclient.Conn
	nc, err := natsclient.DialRetry(natsURL, nodeID, 5, 200*time.Millisecond)
	if err != nil {
		log.Printf("orchestrator: NATS at %s unavailable: %v (mesh features degraded)", natsURL, err)
	}
	var pub mesh.Publisher
	if nc != nil {
		defer nc.Close()
		pub = nc
	}
	meshClient := mesh.New(pub, store, router)
	if err := mesh.SubscribeStrategyUpdates(nc, meshClient); err != nil {
		log.Printf("orchestrator: strategy subscription failed: %v", err)
	}

	// Provisioner (graceful no-op without docker socket)
	prov := provision.New(provision.Config{
		WorkerImage: env("WORKER_IMAGE", "hpo-grid/worker:latest"),
		Replicas:    envInt("WORKER_REPLICAS", 2),
		RedisURL:    redisURL,
		NATSURL:     natsURL,
		Network:     env("DOCKER_NETWORK", "grid"),
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	store.StartHeartbeat(ctx, 10*time.Second)
	meshClient.PublishHello(&store.Node)

	srv := api.NewServer(store, router, meshClient, prov, shardSize)
	httpSrv := &http.Server{
		Addr:              apiAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	log.Printf("orchestrator: node %s listening on %s (redis=%s nats=%s grid=%s)", nodeID, apiAddr, redisAddr, natsURL, gridID)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("orchestrator: http server failed: %v", err)
	}
}
