// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"testing"
	"time"

	"github.com/michielvha/stackweaver/core/models"
)

// The run-task payload names the run's creation as the UTC instant ending in Z, whatever zone the
// loaded value carries (utc-timestamps spec, AC2). Run-task services parse it as RFC 3339; a
// local offset was valid but differed from every other timestamp the platform writes.
func TestRunTaskBase_RunCreatedAtIsUTC(t *testing.T) {
	created := time.Date(2026, 10, 17, 20, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	run := &models.Run{
		ID:          "run-zone-test",
		WorkspaceID: "ws-zone-test",
		CreatedAt:   created,
		Workspace:   models.Workspace{Name: "zone", WorkingDirectory: "infra"},
	}
	stage := &models.TaskStage{Stage: "pre_plan"}
	base := runTaskBase(stage, run, &models.Organization{Name: "acme"}, "https://stackweaver.example")

	if got, want := base.RunCreatedAt, "2026-10-17T18:00:00Z"; got != want {
		t.Errorf("RunCreatedAt = %q, want %q", got, want)
	}
	if got, want := base.RunAppURL, "https://stackweaver.example/app/acme/workspaces/zone/runs/run-zone-test"; got != want {
		t.Errorf("RunAppURL = %q, want %q", got, want)
	}
	if base.WorkspaceWorkingDirectory != "infra" || base.OrganizationName != "acme" || base.Stage != "pre_plan" {
		t.Errorf("shared fields not carried: %+v", base)
	}
}
