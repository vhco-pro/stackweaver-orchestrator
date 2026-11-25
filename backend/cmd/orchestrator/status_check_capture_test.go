// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

// An Azure DevOps pull-request run whose configuration is still being captured gets a pending
// status on its pull request, and a failed one once the capture fails (#938, AC-20). The status
// poster runs against a faked Azure DevOps API: Go's default transport records what it is asked
// to send.

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/michielvha/stackweaver/core/dbtest"
	"github.com/michielvha/stackweaver/core/dbtest/testschema"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/repository"
	"github.com/michielvha/stackweaver/core/services/vcs"
	"gorm.io/gorm"
)

type adoStatusPost struct {
	path, auth, state string
}

// recordingTransport answers every request with 200 and records the Azure DevOps status posts.
type recordingTransport struct {
	mu    sync.Mutex
	posts []adoStatusPost
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body struct {
		State string `json:"state"`
	}
	if req.Body != nil {
		data, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(data, &body)
	}
	r.mu.Lock()
	r.posts = append(r.posts, adoStatusPost{path: req.URL.Host + req.URL.Path, auth: req.Header.Get("Authorization"), state: body.State})
	r.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader([]byte(`{"id":1}`))), Header: http.Header{}, Request: req}, nil
}

func TestPRStatus_AzureDevOpsFollowsTheCapture(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set - skipping integration test")
	}
	db, err := dbtest.Open(t, dsn, &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := testschema.Ensure(db); err != nil {
		t.Fatal(err)
	}
	sfx := uuid.NewString()[:8]
	verified := time.Now()
	org := &models.Organization{ID: uuid.New(), Name: "adostatus-" + sfx}
	proj := &models.Project{ID: uuid.New(), OrganizationID: org.ID, Name: "adostatus-" + sfx}
	conn := &models.VCSConnection{ID: uuid.New(), OrganizationID: org.ID, Provider: models.VCSProviderAzureDevOps, AccessToken: "ado-token-of-x", AccountName: "adoorg-" + sfx, AccountVerifiedAt: &verified}
	ws := &models.Workspace{ProjectID: proj.ID, Name: "adostatus-" + sfx, ExecutionMode: "remote", VCSConnectionID: &conn.ID, VCSRepository: "infra/network", VCSBranch: "main", SpeculativeEnabled: true}
	for _, obj := range []any{org, proj, conn, ws} {
		if err := db.Create(obj).Error; err != nil {
			t.Fatal(err)
		}
	}
	cv := &models.ConfigurationVersion{
		WorkspaceID: ws.ID, Status: models.ConfigurationVersionStatusFetching, Source: "tfe-vcs", Speculative: true,
		CommitHash: "abcdefabcdefabcdefabcdefabcdefabcdefabcd", FetchRef: "refs/heads/feature", PRNumber: 7, SourceBranch: "feature",
	}
	if err := db.Create(cv).Error; err != nil {
		t.Fatal(err)
	}
	run := &models.Run{WorkspaceID: ws.ID, ConfigurationVersionID: &cv.ID, Status: models.RunStatusPending, Operation: models.RunOperationPlanOnly}
	if err := db.Create(run).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Where("id = ?", run.ID).Delete(&models.Run{})
		db.Where("id = ?", cv.ID).Delete(&models.ConfigurationVersion{})
		db.Where("id = ?", ws.ID).Delete(&models.Workspace{})
		db.Where("id = ?", conn.ID).Delete(&models.VCSConnection{})
		db.Where("id = ?", proj.ID).Delete(&models.Project{})
		db.Where("organization_id = ?", org.ID).Delete(&models.AgentPool{})
		db.Where("id = ?", org.ID).Delete(&models.Organization{})
	})

	rec := &recordingTransport{}
	saved := http.DefaultTransport
	http.DefaultTransport = rec
	t.Cleanup(func() { http.DefaultTransport = saved })

	runRepo := repository.NewRunRepository(db)
	post := func() {
		t.Helper()
		var current models.Run
		if err := db.First(&current, "id = ?", run.ID).Error; err != nil {
			t.Fatal(err)
		}
		updatePRStatusCheck(context.Background(), &current, runRepo, repository.NewWorkspaceRepository(db),
			repository.NewConfigurationVersionRepository(db), repository.NewVCSConnectionRepository(db),
			nil, vcs.NewAzureDevOpsStatusService(nil, nil), nil)
	}
	wantPath := "dev.azure.com/" + conn.AccountName + "/infra/_apis/git/repositories/network/pullRequests/7/statuses"
	last := func() adoStatusPost {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.posts) == 0 {
			return adoStatusPost{}
		}
		return rec.posts[len(rec.posts)-1]
	}

	// While the capture is pending, the pull request shows a pending status.
	post()
	got := last()
	if got.path != wantPath || got.auth != "Bearer ado-token-of-x" || got.state != "pending" {
		t.Fatalf("pending capture posted %+v, want pending to %s with the workspace's connection", got, wantPath)
	}

	// A failed capture errors the version and the run; the pull request shows the failure.
	if err := db.Model(&models.ConfigurationVersion{}).Where("id = ?", cv.ID).Update("status", models.ConfigurationVersionStatusErrored).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.Run{}).Where("id = ?", run.ID).Updates(map[string]any{"status": models.RunStatusErrored, "error_message": "capturing the configuration from VCS failed"}).Error; err != nil {
		t.Fatal(err)
	}
	post()
	got = last()
	if got.path != wantPath || (got.state != "failed" && got.state != "error") {
		t.Errorf("failed capture posted %+v, want a failed or error status to %s", got, wantPath)
	}
	if n := len(rec.posts); n != 2 {
		t.Errorf("%d status posts, want 2 (pending, then failed)", n)
	}
}
