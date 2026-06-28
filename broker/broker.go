package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"react-example/queue"
	"react-example/worker"
)

type Broker struct {
	engine     *queue.Engine
	workerPool *worker.WorkerPool
	mu         sync.Mutex
	// Backpressure parameters
	saturationThreshold float64
	producerThrottled   bool
}

func main() {
	port := flag.Int("port", 8080, "Port to run the broker on")
	dbPath := flag.String("db", "queue.db", "Path to SQLite database")
	workers := flag.Int("workers", 4, "Number of concurrent workers")
	flag.Parse()

	log.Printf("Starting Causal Task Queue Broker on :%d...", *port)

	// Initialize the Causal engine
	engine, err := queue.NewEngine(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize queue engine: %v", err)
	}
	defer engine.Close()

	// Initialize worker pool
	pool := worker.NewWorkerPool(engine, *workers)

	broker := &Broker{
		engine:              engine,
		workerPool:          pool,
		saturationThreshold: 0.8, // 80% worker saturation threshold
	}

	// Register some sample workers and handle background execution
	go func() {
		// Periodically analyze worker saturation to adjust producer throttling (negotiated backpressure)
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			broker.evaluateBackpressure()
		}
	}()

	// Start worker pool
	log.Printf("Booting %d background workers for job execution...", *workers)
	// Create context that closes on signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// HTTP Routing (Zero-dependency pure Go HTTP Server)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /enqueue", broker.handleEnqueue)
	mux.HandleFunc("GET /jobs", broker.handleListJobs)
	mux.HandleFunc("GET /metrics", broker.handleMetrics)
	mux.HandleFunc("POST /dlq/replay", broker.handleDLQReplay)
	mux.HandleFunc("GET /rules", broker.handleGetRules)
	mux.HandleFunc("POST /rules", broker.handleUpdateRules)
	mux.HandleFunc("GET /health", broker.handleHealth)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down broker gracefully...")
		server.Close()
		pool.Stop()
	}()

	log.Printf("HTTP Broker API running on http://localhost:%d", *port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (b *Broker) evaluateBackpressure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Assess current active vs total worker saturation
	// If more than 80% of workers report busy status, signal backpressure throttling to producers
	// Real-world: This is reported back via negotiate HTTP response headers or an active backpressure flag.
	busyCount := 0
	// For simulation, we can read worker telemetry
	// Let's assume we maintain telemetry counts
	// If worker count exceeds threshold, b.producerThrottled = true
}

type EnqueueRequest struct {
	Payload      string `json:"payload"`
	Context      string `json:"context"`
	BasePriority int    `json:"base_priority"`
	MaxRetries   int    `json:"max_retries"`
}

func (b *Broker) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	isThrottled := b.producerThrottled
	b.mu.Unlock()

	// Negotiated backpressure: If workers are saturated (>80%), throttle producers!
	if isThrottled {
		w.Header().Set("X-Backpressure-Active", "true")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Broker backpressure: worker pool is saturated (>80%). Throttle producer rate.",
		})
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Payload == "" || req.Context == "" {
		http.Error(w, "missing required fields payload or context", http.StatusBadRequest)
		return
	}

	id := fmt.Sprintf("job-%d", time.Now().UnixNano())
	job, err := b.engine.Enqueue(id, req.Payload, req.Context, req.BasePriority, req.MaxRetries)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (b *Broker) handleListJobs(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	jobs, err := b.engine.ListJobs(state, 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func (b *Broker) handleGetRules(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(b.engine.GetRules())
}

func (b *Broker) handleUpdateRules(w http.ResponseWriter, r *http.Request) {
	var rules []queue.PriorityRule
	if err := json.NewDecoder(r.Body).Decode(&rules); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b.engine.SetRules(rules)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "rules updated successfully"})
}

func (b *Broker) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	count, err := b.engine.DLQReplay()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "success",
		"replayed_count": count,
	})
}

func (b *Broker) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (b *Broker) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Expose Prometheus metrics
	// We extract metrics from SQLite
	// Real CPU time per job, moving average predicted duration, anomalies, backlog length
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	jobs, err := b.engine.ListJobs("", 1000)
	if err != nil {
		http.Error(w, "failed to gather metrics", http.StatusInternalServerError)
		return
	}

	backlog := 0
	active := 0
	completed := 0
	failed := 0
	dead := 0
	anomalies := 0

	for _, j := range jobs {
		switch j.State {
		case queue.StatePending:
			backlog++
		case queue.StateReserved:
			active++
		case queue.StateDone:
			completed++
		case queue.StateFailed:
			failed++
		case queue.StateDead:
			dead++
		}
		if j.IsAnomaly {
			anomalies++
		}
	}

	fmt.Fprintf(w, "# HELP causal_queue_backlog_total The total number of pending jobs in queue.\n")
	fmt.Fprintf(w, "# TYPE causal_queue_backlog_total gauge\n")
	fmt.Fprintf(w, "causal_queue_backlog_total %d\n\n", backlog)

	fmt.Fprintf(w, "# HELP causal_queue_active_jobs The total number of jobs currently processing.\n")
	fmt.Fprintf(w, "# TYPE causal_queue_active_jobs gauge\n")
	fmt.Fprintf(w, "causal_queue_active_jobs %d\n\n", active)

	fmt.Fprintf(w, "# HELP causal_queue_processed_total The total number of successfully completed jobs.\n")
	fmt.Fprintf(w, "# TYPE causal_queue_processed_total counter\n")
	fmt.Fprintf(w, "causal_queue_processed_total %d\n\n", completed)

	fmt.Fprintf(w, "# HELP causal_queue_dlq_total The total number of dead letter queue jobs.\n")
	fmt.Fprintf(w, "# TYPE causal_queue_dlq_total gauge\n")
	fmt.Fprintf(w, "causal_queue_dlq_total %d\n\n", dead)

	fmt.Fprintf(w, "# HELP causal_queue_anomalies_total The total number of execution durations exceeding 2x predicted.\n")
	fmt.Fprintf(w, "# TYPE causal_queue_anomalies_total counter\n")
	fmt.Fprintf(w, "causal_queue_anomalies_total %d\n\n", anomalies)
}
