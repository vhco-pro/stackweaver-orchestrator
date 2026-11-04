// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

// An Ansible job a release before the launchers queued carries no pool, and only the launch stamps
// one (#933, D9, AC58). The orchestrator's sweep gives such a pending job its organization's hosted
// pool, so a launcher is offered it after an upgrade instead of it waiting for ever. A job with a
// pool, and a job that is no longer pending, are left alone.
//
// Gated behind `integration`; skips unless $TEST_DATABASE_URL is set.

//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/michielvha/stackweaver/core/dbtest"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/repository"
	"gorm.io/gorm"
)

func TestPoollessJobSweep(t *testing.T) {
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
	org := &models.Organization{ID: uuid.New(), Name: "poolless-" + sfx}
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "poolless-" + sfx}
	inv := &models.AnsibleInventory{ID: uuid.New(), OrganizationID: org.ID, ProjectID: &proj.ID, Name: "poolless-" + sfx, Type: models.InventoryTypeStatic}
	customer := &models.AgentPool{ID: uuid.New(), OrganizationID: org.ID, Name: "customer-" + sfx}
	for _, obj := range []any{org, proj, inv, customer} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	job := func(status models.AnsibleJobStatus, pool *uuid.UUID) *models.AnsibleJob {
		j := &models.AnsibleJob{
			ID: uuid.New(), ProjectID: proj.ID, InventoryID: inv.ID, Name: "poolless-" + uuid.NewString()[:8],
			JobType: models.AnsibleJobTypeAdHoc, Module: "ping", Status: status, AgentPoolID: pool,
		}
		if err := db.Create(j).Error; err != nil {
			t.Fatal(err)
		}
		return j
	}
	stray := job(models.AnsibleJobStatusPending, nil)
	pooled := job(models.AnsibleJobStatusPending, &customer.ID)
	done := job(models.AnsibleJobStatusSuccessful, nil)
	t.Cleanup(func() {
		db.Where("id IN ?", []uuid.UUID{stray.ID, pooled.ID, done.ID}).Delete(&models.AnsibleJob{})
		db.Where("organization_id = ?", org.ID).Delete(&models.AgentPool{})
		db.Where("id = ?", inv.ID).Delete(&models.AnsibleInventory{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})

	if err := sweepPoollessJobs(t.Context(), db, nil); err != nil {
		t.Fatal(err)
	}
	hosted, _, err := repository.HostedPoolOf(db, org.ID)
	if err != nil {
		t.Fatal(err)
	}
	pool := func(id uuid.UUID) *uuid.UUID {
		var j models.AnsibleJob
		if err := db.Select("agent_pool_id").First(&j, "id = ?", id).Error; err != nil {
			t.Fatal(err)
		}
		return j.AgentPoolID
	}
	if p := pool(stray.ID); p == nil || *p != hosted {
		t.Errorf("the pool-less pending job has pool %v, want the hosted pool %s", p, hosted)
	}
	if p := pool(pooled.ID); p == nil || *p != customer.ID {
		t.Errorf("the job with a pool was moved to %v", p)
	}
	if p := pool(done.ID); p != nil {
		t.Errorf("the finished job was stamped with %s", *p)
	}
}
