package queue

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrDuplicateJob = errors.New("job with identical content hash already exists")
	ErrNoJobFound   = errors.New("no pending job available")
)

type Engine struct {
	db    *sql.DB
	mu    sync.Mutex
	rules []PriorityRule
}

func NewEngine(dbPath string) (*Engine, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}

	// Enable WAL mode for better concurrency in SQLite
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, fmt.Errorf("failed to enable WAL: %w", err)
	}

	// Initialize tables
	if _, err := db.Exec(SQLiteSchema); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &Engine{
		db:    db,
		rules: DefaultRules(),
	}, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) SetRules(rules []PriorityRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = rules
}

func (e *Engine) GetRules() []PriorityRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rules
}

// ComputeHash generates a content hash of the payload for deduplication.
func ComputeHash(payload string) string {
	h := sha256.New()
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// Enqueue adds a new job to the queue, enforcing content deduplication.
func (e *Engine) Enqueue(id, payload, context string, basePriority, maxRetries int) (*Job, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	hash := ComputeHash(payload)

	// Check for deduplication: we cannot enqueue if there's already a pending or reserved job with the same hash
	var existingID, existingState string
	err := e.db.QueryRow("SELECT id, state FROM jobs WHERE content_hash = ? AND state IN ('pending', 'reserved', 'running')", hash).Scan(&existingID, &existingState)
	if err == nil {
		return nil, fmt.Errorf("%w: job %s is currently %s", ErrDuplicateJob, existingID, existingState)
	}

	now := time.Now()
	job := &Job{
		ID:            id,
		ContentHash:   hash,
		Payload:       payload,
		Context:       context,
		State:         StatePending,
		BasePriority:  basePriority,
		MaxRetries:    maxRetries,
		RetriesCount:  0,
		CreatedAt:     now,
		RunAt:         now,
		PredictedTime: e.getPredictedTime(context),
	}

	job.PriorityScore = ComputePriorityScore(job, now, e.rules)

	query := `INSERT INTO jobs (
		id, content_hash, payload, context, state, priority_score, base_priority, max_retries, retries_count, created_at, run_at, predicted_time
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = e.db.Exec(query,
		job.ID, job.ContentHash, job.Payload, job.Context, string(job.State),
		job.PriorityScore, job.BasePriority, job.MaxRetries, job.RetriesCount,
		job.CreatedAt.Format(time.RFC3339), job.RunAt.Format(time.RFC3339), job.PredictedTime,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job: %w", err)
	}

	return job, nil
}

// RecomputePriorities updates all pending jobs' priority scores dynamically.
func (e *Engine) RecomputePriorities() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT id, context, base_priority, created_at FROM jobs WHERE state = 'pending'")
	if err != nil {
		return err
	}
	defer rows.Close()

	now := time.Now()
	type ScoreUpdate struct {
		id    string
		score float64
	}
	var updates []ScoreUpdate

	for rows.Next() {
		var id, context, createdAtStr string
		var basePriority int
		if err := rows.Scan(&id, &context, &basePriority, &createdAtStr); err != nil {
			return err
		}

		createdAt, _ := time.Parse(time.RFC3339, createdAtStr)
		tempJob := &Job{
			Context:      context,
			BasePriority: basePriority,
			CreatedAt:    createdAt,
		}

		score := ComputePriorityScore(tempJob, now, e.rules)
		updates = append(updates, ScoreUpdate{id: id, score: score})
	}

	// Apply updates
	for _, up := range updates {
		if _, err := tx.Exec("UPDATE jobs SET priority_score = ? WHERE id = ?", up.score, up.id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Reserve fetches and locks the highest-priority job available.
func (e *Engine) Reserve(workerID string) (*Job, error) {
	// First recompute priority scores in real time
	if err := e.RecomputePriorities(); err != nil {
		return nil, fmt.Errorf("recomputing priority failed: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	nowStr := now.Format(time.RFC3339)

	var job Job
	var createdAtStr, runAtStr string

	query := `
		SELECT id, content_hash, payload, context, state, priority_score, base_priority, max_retries, retries_count, created_at, run_at, predicted_time
		FROM jobs
		WHERE state = 'pending' AND run_at <= ?
		ORDER BY priority_score DESC, run_at ASC
		LIMIT 1
	`
	err = tx.QueryRow(query, nowStr).Scan(
		&job.ID, &job.ContentHash, &job.Payload, &job.Context, &job.State,
		&job.PriorityScore, &job.BasePriority, &job.MaxRetries, &job.RetriesCount,
		&createdAtStr, &runAtStr, &job.PredictedTime,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoJobFound
		}
		return nil, err
	}

	job.CreatedAt, _ = time.Parse(time.RFC3339, createdAtStr)
	job.RunAt, _ = time.Parse(time.RFC3339, runAtStr)

	// Reserve it
	reservedAt := time.Now()
	job.ReservedAt = &reservedAt
	job.State = StateReserved

	_, err = tx.Exec("UPDATE jobs SET state = 'reserved', reserved_at = ? WHERE id = ?", reservedAt.Format(time.RFC3339), job.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &job, nil
}

// Complete marks a job as done and records actual metrics.
func (e *Engine) Complete(jobID string, cpuDuration float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var context string
	var predicted float64
	err = tx.QueryRow("SELECT context, predicted_time FROM jobs WHERE id = ?", jobID).Scan(&context, &predicted)
	if err != nil {
		return err
	}

	now := time.Now()
	isAnomaly := cpuDuration > (2.0 * predicted)

	_, err = tx.Exec(`
		UPDATE jobs 
		SET state = 'done', completed_at = ?, real_cpu_time = ?, is_anomaly = ? 
		WHERE id = ?`,
		now.Format(time.RFC3339), cpuDuration, isAnomaly, jobID,
	)
	if err != nil {
		return err
	}

	// Update the moving average of execution time for this context
	var avgDuration float64
	var count int
	err = tx.QueryRow("SELECT avg_duration, execution_count FROM job_metrics WHERE context = ?", context).Scan(&avgDuration, &count)
	if errors.Is(err, sql.ErrNoRows) {
		// First metric
		_, err = tx.Exec("INSERT INTO job_metrics (context, avg_duration, execution_count) VALUES (?, ?, ?)", context, cpuDuration, 1)
	} else if err == nil {
		// Compute moving average: (avg * count + current) / (count + 1)
		newCount := count + 1
		newAvg := (avgDuration*float64(count) + cpuDuration) / float64(newCount)
		_, err = tx.Exec("UPDATE job_metrics SET avg_duration = ?, execution_count = ? WHERE context = ?", newAvg, newCount, context)
	}
	if err != nil {
		return err
	}

	// Update this job's internal record to match the new predicted time for future jobs
	return tx.Commit()
}

// Fail records execution failures, schedules retries, or moves to Dead Letter Queue (DLQ).
func (e *Engine) Fail(jobID, errMsg string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var retries, maxRetries int
	var context string
	err := e.db.QueryRow("SELECT retries_count, max_retries, context FROM jobs WHERE id = ?", jobID).Scan(&retries, &maxRetries, &context)
	if err != nil {
		return err
	}

	newRetries := retries + 1
	if newRetries <= maxRetries {
		// Exponential backoff: 2 ^ retries seconds
		backoffSeconds := math.Pow(2, float64(newRetries))
		runAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

		_, err = e.db.Exec(`
			UPDATE jobs 
			SET state = 'pending', retries_count = ?, run_at = ?, last_error = ? 
			WHERE id = ?`,
			newRetries, runAt.Format(time.RFC3339), errMsg, jobID,
		)
	} else {
		// Exceeded retries, push to Dead Letter Queue (DLQ)
		_, err = e.db.Exec(`
			UPDATE jobs 
			SET state = 'dead', retries_count = ?, last_error = ? 
			WHERE id = ?`,
			newRetries, errMsg, jobID,
		)
	}

	return err
}

// DLQReplay replays all failed dead letter jobs.
func (e *Engine) DLQReplay() (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	nowStr := time.Now().Format(time.RFC3339)
	res, err := e.db.Exec(`
		UPDATE jobs 
		SET state = 'pending', retries_count = 0, run_at = ?, last_error = NULL 
		WHERE state = 'dead'`,
		nowStr,
	)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	return int(affected), err
}

// Helper: retrieve predicted time for context from historical metrics
func (e *Engine) getPredictedTime(context string) float64 {
	var avg float64
	err := e.db.QueryRow("SELECT avg_duration FROM job_metrics WHERE context = ?", context).Scan(&avg)
	if err != nil {
		return 1.5 // Default to 1.5s if no historical metrics exist yet
	}
	return avg
}

// ListJobs fetches jobs based on filters (for dashboard).
func (e *Engine) ListJobs(state string, limit int) ([]*Job, error) {
	var rows *sql.Rows
	var err error

	if state == "" {
		rows, err = e.db.Query("SELECT id, content_hash, payload, context, state, priority_score, base_priority, max_retries, retries_count, last_error, created_at, run_at, reserved_at, completed_at, predicted_time, real_cpu_time, is_anomaly FROM jobs ORDER BY created_at DESC LIMIT ?", limit)
	} else {
		rows, err = e.db.Query("SELECT id, content_hash, payload, context, state, priority_score, base_priority, max_retries, retries_count, last_error, created_at, run_at, reserved_at, completed_at, predicted_time, real_cpu_time, is_anomaly FROM jobs WHERE state = ? ORDER BY priority_score DESC, created_at DESC LIMIT ?", state, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		var j Job
		var cr, ru string
		var re, co sql.NullString
		var lastErr sql.NullString
		var anomalyInt int

		err := rows.Scan(
			&j.ID, &j.ContentHash, &j.Payload, &j.Context, &j.State, &j.PriorityScore, &j.BasePriority,
			&j.MaxRetries, &j.RetriesCount, &lastErr, &cr, &ru, &re, &co, &j.PredictedTime, &j.RealCPUTime, &anomalyInt,
		)
		if err != nil {
			return nil, err
		}

		j.CreatedAt, _ = time.Parse(time.RFC3339, cr)
		j.RunAt, _ = time.Parse(time.RFC3339, ru)
		if re.Valid {
			t, _ := time.Parse(time.RFC3339, re.String)
			j.ReservedAt = &t
		}
		if co.Valid {
			t, _ := time.Parse(time.RFC3339, co.String)
			j.CompletedAt = &t
		}
		if lastErr.Valid {
			j.LastError = lastErr.String
		}
		j.IsAnomaly = anomalyInt == 1

		jobs = append(jobs, &j)
	}

	return jobs, nil
}
