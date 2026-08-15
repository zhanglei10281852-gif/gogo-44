package store

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/model"
)

func storeConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SnapshotEvery = 100
	return cfg
}

func validJob() *model.Job {
	now := time.Unix(100, 0).UTC()
	return &model.Job{ID: "one", Queue: "default", Type: "test", Payload: []byte(`null`), State: model.StateReady, CreatedAt: now, UpdatedAt: now, AvailableAt: now, MaxAttempts: 1, Backoff: model.BackoffPolicy{Kind: "fixed", BaseSeconds: 0, MaxSeconds: 0}, Resources: model.Resources{Slots: 1}, History: []model.Transition{{To: model.StateReady, At: now, Reason: "created"}}}
}

func TestJournalTamperDetected(t *testing.T) {
	cfg := storeConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(validJob()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.JournalPath())
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if data[i] == 'o' {
			data[i] = 'x'
			break
		}
	}
	if err := os.WriteFile(cfg.JournalPath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(cfg.JournalPath()); err == nil {
		t.Fatal("expected tamper detection")
	}
}

func TestRecoverRejectsSnapshotStateNotDerivedFromJournalPrefix(t *testing.T) {
	cfg := storeConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	job := validJob()
	job.IdempotencyKey = "key-one"
	if err := store.Create(job); err != nil {
		t.Fatal(err)
	}
	if err := store.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot, err := InspectSnapshot(cfg.SnapshotPath())
	if err != nil {
		t.Fatal(err)
	}
	events, err := JournalEvents(cfg.JournalPath())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Sequence != 1 || len(events) != 1 || snapshot.LastHash != events[0].Hash {
		t.Fatalf("unexpected fixture markers: snapshot=%+v events=%d", snapshot, len(events))
	}
	snapshot.Jobs["one"].Priority = 77
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.SnapshotPath(), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Recover(cfg); err == nil {
		t.Fatal("recovery trusted snapshot state that was not derived from the journal prefix")
	}
}
