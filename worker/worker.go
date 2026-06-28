package worker

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"react-example/queue" // or queue package relative import
)

// TaskFunc defines the executable logic for a job
type TaskFunc func(payload string) error

type WorkerPool struct {
	engine        *queue.Engine
	workerCount   int
	handlers      map[string]TaskFunc
	telemetryChan chan Telemetry
	stopChan      chan struct{}
}

type Telemetry struct {
	WorkerID  int     `json:"worker_id"`
	CPULoad   float64 `json:"cpu_load"`   // Simulated or real CPU usage (0.0 to 1.0)
	MemUsage  float64 `json:"mem_usage"`  // Simulated or real memory saturation (0.0 to 1.0)
	BusyState bool    `json:"busy_state"`
}

func NewWorkerPool(engine *queue.Engine, workerCount int) *WorkerPool {
	return &WorkerPool{
		engine:        engine,
		workerCount:   workerCount,
		handlers:      make(map[string]TaskFunc),
		telemetryChan: make(chan Telemetry, 100),
		stopChan:      make(chan struct{}),
	}
}

func (wp *WorkerPool) RegisterHandler(context string, handler TaskFunc) {
	wp.handlers[context] = handler
}

func (wp *WorkerPool) Start(ctx context.Context) {
	// Start individual worker goroutines
	for i := 1; i <= wp.workerCount; i++ {
		go wp.runWorker(ctx, i)
	}

	// Start telemetry reporter (intelligent backpressure monitor)
	go wp.reportTelemetry(ctx)
}

func (wp *WorkerPool) Stop() {
	close(wp.stopChan)
}

func (wp *WorkerPool) TelemetryChan() <-chan Telemetry {
	return wp.telemetryChan
}

func (wp *WorkerPool) runWorker(ctx context.Context, id int) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Track worker utilization
	var activeJob bool

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		case <-ticker.C:
			// Fetch and reserve job
			job, err := wp.engine.Reserve(fmt.Sprintf("worker-%d", id))
			if err != nil {
				// No job found, report worker idleness
				if activeJob {
					wp.telemetryChan <- Telemetry{WorkerID: id, CPULoad: wp.getSimulatedCPU(false), MemUsage: wp.getSimulatedMem(false), BusyState: false}
					activeJob = false
				}
				continue
			}

			activeJob = true
			wp.telemetryChan <- Telemetry{WorkerID: id, CPULoad: wp.getSimulatedCPU(true), MemUsage: wp.getSimulatedMem(true), BusyState: true}

			// Measure real CPU Time
			cpuStart := time.Now()

			// Lookup registered handler or fall back to default simulation
			handler, exists := wp.handlers[job.Context]
			var runErr error

			if exists {
				runErr = handler(job.Payload)
			} else {
				// Default task processing simulator with random delays and simulated failures
				runErr = wp.simulateTaskExecution(job)
			}

			cpuEnd := time.Now()
			realCPUTime := cpuEnd.Sub(cpuStart).Seconds()

			if runErr != nil {
				// Record failure
				_ = wp.engine.Fail(job.ID, runErr.Error())
			} else {
				// Record successful completion
				_ = wp.engine.Complete(job.ID, realCPUTime)
			}

			wp.telemetryChan <- Telemetry{WorkerID: id, CPULoad: wp.getSimulatedCPU(false), MemUsage: wp.getSimulatedMem(false), BusyState: false}
			activeJob = false
		}
	}
}

// Simulate actual workload execution based on job characteristics
func (wp *WorkerPool) simulateTaskExecution(job *queue.Job) error {
	var duration time.Duration

	switch job.Context {
	case "payment_failed":
		// Quick resolution but high success priority
		duration = time.Duration(100+rand.Intn(200)) * time.Millisecond
	case "report_generation":
		// Heavy CPU task, sometimes runs into anomalies (e.g. huge data payload)
		baseDuration := 800 * time.Millisecond
		if rand.Float64() < 0.15 { // 15% chance of an execution anomaly (runs 3x longer)
			duration = baseDuration * 3
		} else {
			duration = baseDuration + time.Duration(rand.Intn(400))*time.Millisecond
		}
	case "user_onboarding":
		// Fast, standard database write task
		duration = time.Duration(150+rand.Intn(150)) * time.Millisecond
	default:
		duration = time.Duration(200+rand.Intn(300)) * time.Millisecond
	}

	// Simulated processing delay
	time.Sleep(duration)

	// Simulating random business failures for testing exponential backoff
	if job.Context == "payment_failed" && job.RetriesCount == 0 && rand.Float64() < 0.4 {
		return fmt.Errorf("temporary stripe timeout connection closed")
	}

	if rand.Float64() < 0.05 { // 5% flat failure rate for other tasks
		return fmt.Errorf("unexpected database transaction deadlock")
	}

	return nil
}

// Retrieve simulated CPU load depending on whether worker is currently busy
func (wp *WorkerPool) getSimulatedCPU(busy bool) float64 {
	if !busy {
		return 0.05 + rand.Float64()*0.05 // 5% - 10% idle CPU
	}
	// Busy workers spike CPU based on context complexity
	return 0.55 + rand.Float64()*0.35 // 55% - 90% active CPU
}

func (wp *WorkerPool) getSimulatedMem(busy bool) float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	systemMemRatio := float64(m.Alloc) / 1024.0 / 1024.0 / 128.0 // Normalized relative to 128MB
	if systemMemRatio > 1.0 {
		systemMemRatio = 0.95
	}

	if !busy {
		return 0.2 + systemMemRatio*0.1
	}
	return 0.5 + systemMemRatio*0.2
}

func (wp *WorkerPool) reportTelemetry(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		case <-ticker.C:
			// Standard heartbeat reporting
			wp.telemetryChan <- Telemetry{
				WorkerID:  0, // Aggregate reporting
				CPULoad:   wp.getSimulatedCPU(false),
				MemUsage:  wp.getSimulatedMem(false),
				BusyState: false,
			}
		}
	}
}
