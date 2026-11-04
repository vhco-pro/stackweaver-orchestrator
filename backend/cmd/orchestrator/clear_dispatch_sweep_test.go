// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

// Runs a retired queue runner was handed (claimed for dispatch, never started) are re-dispatched
// (#933, D14, AC57, AC58): at the orchestrator's start every such run, and on the stuck-run sweep
// one claimed five minutes ago or more, so a run an old orchestrator enqueued during a rolling
// upgrade is not stranded. A run a launcher holds is left alone.
//
// Gated behind `integration`; skips unless $TEST_DATABASE_URL is set.

//go:build integration

package main

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/michielvha/stackweaver/core/dbtest"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/repository"
	"gorm.io/gorm"
)

func TestClearDispatchSweep(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set - skipping integration test")
	}
	db, err := dbtest.Open(t, dsn, &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := models.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	sfx := uuid.NewString()[:8]
	org := &models.Organization{ID: uuid.New(), Name: "sweep-" + sfx}
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "sweep-" + sfx}
	ws := &models.Workspace{ProjectID: proj.ID, Name: "sweep-" + sfx, ExecutionMode: "remote"}
	runner := &models.Runner{ID: uuid.New(), Kind: models.RunnerKindHosted, HostedGroup: "sweep-" + sfx, Name: "sweep-" + sfx, Status: models.RunnerStatusOnline}
	for _, obj := range []any{org, proj, ws, runner} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	at := func(ago time.Duration) *time.Time { v := time.Now().Add(-ago); return &v }
	recent := &models.Run{WorkspaceID: ws.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly, DispatchedAt: at(time.Minute)}
	old := &models.Run{WorkspaceID: ws.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly, DispatchedAt: at(6 * time.Minute)}
	held := &models.Run{WorkspaceID: ws.ID, Status: models.RunStatusPlanning, Operation: models.RunOperationPlanOnly, DispatchedAt: at(10 * time.Minute), RunnerID: &runner.ID}
	for _, r := range []*models.Run{recent, old, held} {
		if err := db.Create(r).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		db.Where("id IN ?", []string{recent.ID, old.ID, held.ID}).Delete(&models.Run{})
		db.Where("id = ?", runner.ID).Delete(&models.Runner{})
		db.Where("id = ?", ws.ID).Delete(&models.Workspace{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})
	runRepo := repository.NewRunRepository(db)
	dispatched := func(id string) bool {
		var r models.Run
		if err := db.Select("dispatched_at").First(&r, "id = ?", id).Error; err != nil {
			t.Fatal(err)
		}
		return r.DispatchedAt != nil
	}

	// The periodic sweep re-dispatches only a run claimed five minutes ago or more.
	if err := sweepQueueDispatches(runRepo, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if dispatched(old.ID) || !dispatched(recent.ID) {
		t.Errorf("after the periodic sweep: old dispatched=%t, recent dispatched=%t; want only the old one released", dispatched(old.ID), dispatched(recent.ID))
	}
	// The start sweep re-dispatches every one; the run a launcher holds keeps its claim.
	if err := sweepQueueDispatches(runRepo, 0); err != nil {
		t.Fatal(err)
	}
	if dispatched(recent.ID) || !dispatched(held.ID) {
		t.Errorf("after the start sweep: recent dispatched=%t, held dispatched=%t", dispatched(recent.ID), dispatched(held.ID))
	}
}
