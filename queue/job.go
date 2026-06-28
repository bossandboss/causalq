package queue

import (
	"time"
)

type JobState string

const (
	StatePending  JobState = "pending"
	StateReserved JobState = "reserved"
	StateRunning  JobState = "running"
	StateDone     JobState = "done"
	StateFailed   JobState = "failed"
	StateDead     JobState = "dead"
)

type Job struct {
	ID             string     `json:"id"`
	ContentHash    string     `json:"content_hash"`
	Payload        string     `json:"payload"`
	Context        string     `json:"context"` // e.g. "payment_failed", "report_generation", "user_onboarding"
	State          JobState   `json:"state"`
	PriorityScore  float64    `json:"priority_score"`
	BasePriority   int        `json:"base_priority"`
	MaxRetries     int        `json:"max_retries"`
	RetriesCount   int        `json:"retries_count"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	RunAt          time.Time  `json:"run_at"`
	ReservedAt     *time.Time `json:"reserved_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	PredictedTime  float64    `json:"predicted_time"` // Predicted execution time in seconds (moving average)
	RealCPUTime    float64    `json:"real_cpu_time"`  // Actual CPU time in seconds
	IsAnomaly      bool       `json:"is_anomaly"`     // True if actual > 2 * predicted
}
