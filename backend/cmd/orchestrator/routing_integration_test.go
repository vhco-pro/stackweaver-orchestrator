// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

// The orchestrator routes runs instead of enqueuing them (#933, D9, AC50): a remote workspace's
// run is stamped with its organization's hosted pool, an agent workspace's with its pool, and
// nothing is pushed on the Redis runs list.
//
// Gated behind `integration`; skips unless $TEST_DATABASE_URL is set.

//go:build integration

package main

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/michielvha/stackweaver/core/dbtest"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/queue"
	"github.com/michielvha/stackweaver/core/repository"
	"gorm.io/gorm"
)

func TestProcessPendingRuns_RoutesAndEnqueuesNothing(t *testing.T) {
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
	mr := miniredis.RunT(t)
	host, portStr, _ := strings.Cut(mr.Addr(), ":")
	port, _ := strconv.Atoi(portStr)
	q, err := queue.NewRedisQueue(host, port, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	sfx := uuid.NewString()[:8]
	org := &models.Organization{ID: uuid.New(), Name: "route-" + sfx}
	hosted := models.NewHostedPool(org.ID)
	customer := &models.AgentPool{ID: uuid.New(), OrganizationID: org.ID, Name: "own", OrganizationScoped: true}
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "route-" + sfx}
	remote := &models.Workspace{ProjectID: proj.ID, Name: "route-remote-" + sfx, ExecutionMode: "remote"}
	agent := &models.Workspace{ProjectID: proj.ID, Name: "route-agent-" + sfx, ExecutionMode: "agent", AgentPoolID: &customer.ID}
	for _, obj := range []any{org, hosted, customer, proj, remote, agent} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	runRemote := &models.Run{WorkspaceID: remote.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
	runAgent := &models.Run{WorkspaceID: agent.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
	for _, r := range []*models.Run{runRemote, runAgent} {
		if err := db.Create(r).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		db.Where("id IN ?", []string{runRemote.ID, runAgent.ID}).Delete(&models.Run{})
		db.Where("id IN ?", []string{remote.ID, agent.ID}).Delete(&models.Workspace{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("organization_id = ?", org.ID).Delete(&models.AgentPool{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})

	// The pending list is shared with whatever else the database holds; the runs of this test are
	// among the first ten only if routed directly, so route them the way processPendingRuns does.
	runRepo := repository.NewRunRepository(db)
	for _, r := range []*models.Run{runRemote, runAgent} {
		reloaded, err := runRepo.GetByID(r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := routeRun(t.Context(), db, q.Client(), reloaded); err != nil {
			t.Fatal(err)
		}
	}
	if err := processPendingRuns(t.Context(), db, q, runRepo); err != nil {
		t.Fatal(err)
	}
	for r, want := range map[*models.Run]uuid.UUID{runRemote: hosted.ID, runAgent: customer.ID} {
		var stored models.Run
		if err := db.First(&stored, "id = ?", r.ID).Error; err != nil {
			t.Fatal(err)
		}
		if stored.AgentPoolID == nil || *stored.AgentPoolID != want {
			t.Errorf("run %s routed to %v, want %s", r.ID, stored.AgentPoolID, want)
		}
		if stored.DispatchedAt != nil {
			t.Errorf("run %s was claimed for a queue dispatch", r.ID)
		}
	}
	if n, _ := q.Client().LLen(t.Context(), "runs").Result(); n != 0 {
		t.Errorf("%d runs enqueued on the Redis list, want none", n)
	}
}

// A pending run of a VCS-backed workspace without a configuration version gets one
// configuration_snapshot work item, however often it is seen; a run with one, or of a workspace
// without VCS, gets none (#933, decision 18).
func TestRequestRunConfiguration_OneSnapshotPerRun(t *testing.T) {
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
	org := &models.Organization{ID: uuid.New(), Name: "routecfg-" + sfx}
	hosted := models.NewHostedPool(org.ID)
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "routecfg-" + sfx}
	conn := &models.VCSConnection{ID: uuid.New(), OrganizationID: org.ID, Provider: models.VCSProviderGitLab, AccessToken: "unused", AccountName: "routecfg-" + sfx}
	vcsWS := &models.Workspace{ProjectID: proj.ID, Name: "routecfg-vcs-" + sfx, ExecutionMode: "remote", VCSConnectionID: &conn.ID, VCSRepository: "acme/infra", VCSBranch: "main"}
	plainWS := &models.Workspace{ProjectID: proj.ID, Name: "routecfg-plain-" + sfx, ExecutionMode: "remote"}
	for _, obj := range []any{org, hosted, proj, conn, vcsWS, plainWS} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	needs := &models.Run{WorkspaceID: vcsWS.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
	plain := &models.Run{WorkspaceID: plainWS.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
	for _, r := range []*models.Run{needs, plain} {
		if err := db.Create(r).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		db.Where("organization_id = ?", org.ID).Delete(&models.WorkItem{})
		db.Where("id IN ?", []string{needs.ID, plain.ID}).Delete(&models.Run{})
		db.Where("workspace_id IN ?", []string{vcsWS.ID, plainWS.ID}).Delete(&models.ConfigurationVersion{})
		db.Where("id IN ?", []string{vcsWS.ID, plainWS.ID}).Delete(&models.Workspace{})
		db.Where("id = ?", conn.ID).Delete(&models.VCSConnection{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("organization_id = ?", org.ID).Delete(&models.AgentPool{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})

	for range 2 {
		for _, r := range []*models.Run{needs, plain} {
			if err := requestRunConfiguration(t.Context(), db, nil, r); err != nil {
				t.Fatal(err)
			}
		}
	}
	var items []models.WorkItem
	db.Where("organization_id = ?", org.ID).Find(&items)
	if len(items) != 1 || items[0].Kind != models.WorkItemConfigurationSnapshot || items[0].RunID == nil || *items[0].RunID != needs.ID {
		t.Errorf("work items = %+v, want one configuration snapshot of run %s", items, needs.ID)
	}
	if len(items) == 1 && (items[0].AgentPoolID == nil || *items[0].AgentPoolID != hosted.ID) {
		t.Errorf("the configuration snapshot is on pool %v, want the hosted pool", items[0].AgentPoolID)
	}
}

// Runs from before #938, or written by an older API replica during a rolling deploy, can be VCS
// runs with no configuration version, with or without a capture already waiting or running. The
// orchestrator converts each: it gives the run a fetching configuration version for its branch head
// and keeps the run's existing capture, or requests one (AC-19). A run another process converted
// first is left as it is.
func TestRequestRunConfiguration_ConvertsRunsFromBeforeTheUpgrade(t *testing.T) {
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
	org := &models.Organization{ID: uuid.New(), Name: "routeup-" + sfx}
	hosted := models.NewHostedPool(org.ID)
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "routeup-" + sfx}
	conn := &models.VCSConnection{ID: uuid.New(), OrganizationID: org.ID, Provider: models.VCSProviderGitLab, AccessToken: "unused", AccountName: "routeup-" + sfx}
	ws := &models.Workspace{ProjectID: proj.ID, Name: "routeup-" + sfx, ExecutionMode: "remote", VCSConnectionID: &conn.ID, VCSRepository: "acme/infra", VCSBranch: "main"}
	for _, obj := range []any{org, hosted, proj, conn, ws} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	runs := map[string]*models.Run{}
	for _, name := range []string{"none", "pending", "running"} {
		r := &models.Run{WorkspaceID: ws.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
		if err := db.Create(r).Error; err != nil {
			t.Fatal(err)
		}
		runs[name] = r
	}
	existing := map[string]*models.WorkItem{}
	for name, status := range map[string]string{"pending": models.WorkItemStatusPending, "running": models.WorkItemStatusRunning} {
		runID := runs[name].ID
		item := &models.WorkItem{
			ID: "wi-up" + sfx + name[:1], OrganizationID: org.ID, ProjectID: &proj.ID, Kind: models.WorkItemConfigurationSnapshot,
			RunID: &runID, TriggeredBy: "run", Status: status, AgentPoolID: &hosted.ID,
		}
		if err := db.Create(item).Error; err != nil {
			t.Fatal(err)
		}
		existing[name] = item
	}
	t.Cleanup(func() {
		db.Where("organization_id = ?", org.ID).Delete(&models.WorkItem{})
		for _, r := range runs {
			db.Where("id = ?", r.ID).Delete(&models.Run{})
		}
		db.Where("workspace_id = ?", ws.ID).Delete(&models.ConfigurationVersion{})
		db.Where("id = ?", ws.ID).Delete(&models.Workspace{})
		db.Where("id = ?", conn.ID).Delete(&models.VCSConnection{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("organization_id = ?", org.ID).Delete(&models.AgentPool{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})

	for range 2 {
		for _, r := range runs {
			if err := requestRunConfiguration(t.Context(), db, nil, r); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, r := range runs {
		var reloaded models.Run
		db.First(&reloaded, "id = ?", r.ID)
		if reloaded.ConfigurationVersionID == nil {
			t.Errorf("run %q was not given a configuration version", name)
			continue
		}
		var cv models.ConfigurationVersion
		db.First(&cv, "id = ?", *reloaded.ConfigurationVersionID)
		if cv.Status != models.ConfigurationVersionStatusFetching || cv.CommitHash != "" || cv.Source != "tfe-vcs" {
			t.Errorf("run %q's configuration version = status %s commit %q source %s, want fetching at the branch head", name, cv.Status, cv.CommitHash, cv.Source)
		}
		var items []models.WorkItem
		db.Where("run_id = ? AND kind = ?", r.ID, models.WorkItemConfigurationSnapshot).Find(&items)
		if len(items) != 1 {
			t.Errorf("run %q has %d captures, want 1", name, len(items))
			continue
		}
		if e, ok := existing[name]; ok && items[0].ID != e.ID {
			t.Errorf("run %q's waiting capture %s was replaced by %s", name, e.ID, items[0].ID)
		}
	}
	var cvs int64
	db.Model(&models.ConfigurationVersion{}).Where("workspace_id = ?", ws.ID).Count(&cvs)
	if cvs != 3 {
		t.Errorf("%d configuration versions for three runs seen twice, want 3", cvs)
	}
}
