package queue

import (
	"time"
)

type RuleType string

const (
	RuleTypeBoost              RuleType = "boost"               // Absolute score boost
	RuleTypeStarvationMitigation RuleType = "starvation_boost"    // Boost score per second in queue
	RuleTypeContextOutranksAge RuleType = "context_outranks_age" // Outranks other jobs older/younger than X seconds
)

type PriorityRule struct {
	ID          string   `json:"id"`
	Context     string   `json:"context"`      // e.g. "payment_failed"
	Type        RuleType `json:"type"`         // e.g. RuleTypeBoost, RuleTypeContextOutranksAge
	Value       float64  `json:"value"`        // Boost amount, or age threshold in seconds
	TargetClass string   `json:"target_class"` // e.g. "all", or other contexts
}

// DefaultRules provides a set of initial demonstration rules.
func DefaultRules() []PriorityRule {
	return []PriorityRule{
		{
			ID:          "rule-1",
			Context:     "payment_failed",
			Type:        RuleTypeBoost,
			Value:       500.0, // Critical business failure gets huge priority
			TargetClass: "all",
		},
		{
			ID:          "rule-2",
			Context:     "user_onboarding",
			Type:        RuleTypeContextOutranksAge,
			Value:       30.0, // User onboarding outranks general jobs (like report_generation) older than 30s
			TargetClass: "report_generation",
		},
		{
			ID:          "rule-3",
			Context:     "report_generation",
			Type:        RuleTypeStarvationMitigation,
			Value:       0.5, // Adds 0.5 points per second in queue to prevent starvation
			TargetClass: "all",
		},
	}
}

// ComputePriorityScore calculates the real-time priority score for a job based on active rules.
func ComputePriorityScore(job *Job, now time.Time, rules []PriorityRule) float64 {
	score := float64(job.BasePriority)
	age := now.Sub(job.CreatedAt).Seconds()
	if age < 0 {
		age = 0
	}

	// Base aging: every job gets a tiny linear boost to ensure older jobs eventually run
	// Default base starvation boost: 0.1 points per second in queue
	starvationRate := 0.1

	// Apply custom rules
	for _, rule := range rules {
		switch rule.Type {
		case RuleTypeBoost:
			if job.Context == rule.Context {
				score += rule.Value
			}
		case RuleTypeStarvationMitigation:
			if rule.Context == "all" || job.Context == rule.Context {
				starvationRate = rule.Value
			}
		case RuleTypeContextOutranksAge:
			if job.Context == rule.Context {
				// Outranks other jobs that have been in the queue up to Value seconds
				// We give a boost equivalent to the starvation rate * value
				score += rule.Value * starvationRate * 10.0 // Add a substantial weight
			}
		}
	}

	// Add dynamic starvation/aging score
	score += age * starvationRate

	return score
}
