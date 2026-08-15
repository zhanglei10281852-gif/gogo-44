package planner

import (
	"testing"
	"time"

	"QueueForge/internal/model"
)

func TestBuildHonorsDependencies(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	job := func(id string, deps ...string) *model.Job {
		return &model.Job{ID: id, Queue: "q", Type: "work", State: model.StateBlocked, CreatedAt: now, UpdatedAt: now, AvailableAt: now, Dependencies: deps, MaxAttempts: 1, Backoff: model.BackoffPolicy{Kind: "fixed"}, Resources: model.Resources{Slots: 1}, Payload: []byte(`null`), History: []model.Transition{{To: model.StateBlocked, At: now, Reason: "test"}}}
	}
	worker := model.Worker{ID: "w", Queues: []string{"q"}, Capacity: model.Resources{Slots: 1}}
	plan, err := Build([]*model.Job{job("a"), job("b", "a")}, PlanRequest{Workers: []model.Worker{worker}, Durations: []DurationEstimate{{JobType: "work", Seconds: 10}}, StartAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Assignments) != 2 {
		t.Fatalf("assignments=%d", len(plan.Assignments))
	}
	if plan.Assignments[1].StartAt.Before(plan.Assignments[0].FinishAt) {
		t.Fatal("dependent job overlaps dependency")
	}
}
