// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/michielvha/logger"
	terraformHandlers "github.com/michielvha/stackweaver/backend/internal/api/v2/handlers/terraform"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/repository"
	"github.com/michielvha/stackweaver/core/services/runtask"
)

// Run-task stage driver (the 7th orchestrator worker). Each tick:
//
//  1. runs `pending` with a pending pre_plan stage move to pre_plan_running (the dispatch claim
//     refuses gated runs, so this is what opens the pre_plan gate's webhook phase);
//  2. stages whose run reached the matching *_running status are CLAIMED (a pending→running
//     conditional UPDATE that exactly one orchestrator instance wins) and their webhooks fired,
//     one signed POST per task result, each carrying a freshly minted access token;
//  3. finalize backstop: running stages whose results are all terminal are folded by the shared
//     engine (the callback handler is the primary finalizer; this catches its failures);
//  4. timeout sweep: no progress for taskResultTimeout (default 10m, TFE contract) or running
//     longer than taskResultMaxDuration (default 60m) errors the remaining results and finalizes
//     (mandatory → run fails; advisory-only → stage passes).
//
// Timeouts are env-tunable (STACKWEAVER_TASK_RESULT_TIMEOUT / _MAX_DURATION, Go durations) so the
// runtime verification can exercise the sweep without waiting 10 minutes.

func taskTimeouts() (progress, cap time.Duration) {
	progress, cap = 10*time.Minute, 60*time.Minute
	if v := os.Getenv("STACKWEAVER_TASK_RESULT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			progress = d
		}
	}
	if v := os.Getenv("STACKWEAVER_TASK_RESULT_MAX_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cap = d
		}
	}
	return progress, cap
}

// taskStageDeps bundles what the worker needs per tick.
type taskStageDeps struct {
	runRepo         *repository.RunRepository
	stageRepo       *repository.TaskStageRepository
	resultRepo      *repository.TaskResultRepository
	configVerRepo   *repository.ConfigurationVersionRepository
	orgRepo         *repository.OrganizationRepository
	userRepo        *repository.UserRepository
	taskSvc         *runtask.Service
	engine          *runtask.Engine
	appURL          string // public UI base (run_app_url / workspace_app_url)
	apiURL          string // public API base (callback / plan json / config download URLs)
	progressTimeout time.Duration
	maxDuration     time.Duration
}

func processTaskStages(ctx context.Context, d *taskStageDeps) {
	// 1. Open pre_plan gates: pending runs with a pending pre_plan stage start their task phase.
	if awaiting, err := d.stageRepo.ListPrePlanAwaiting(20); err != nil {
		logger.Warnf("task stages: list pre_plan awaiting: %v", err)
	} else {
		for i := range awaiting {
			ok, err := d.runRepo.TransitionStatus(awaiting[i].RunID,
				[]models.RunStatus{models.RunStatusPending}, models.RunStatusPrePlanRunning, nil)
			if err != nil {
				logger.Warnf("task stages: move run %s to pre_plan_running: %v", awaiting[i].RunID, err)
			} else if ok {
				logger.Infof("run %s entered pre_plan task stage", awaiting[i].RunID)
			}
		}
	}

	// 2. Claim startable stages and fire their webhooks.
	startable, err := d.stageRepo.ListStartable(20)
	if err != nil {
		logger.Warnf("task stages: list startable: %v", err)
		return
	}
	for i := range startable {
		stage := &startable[i]
		claimed, err := d.stageRepo.ClaimStart(stage.ID)
		if err != nil {
			logger.Warnf("task stages: claim stage %s: %v", stage.ID, err)
			continue
		}
		if !claimed {
			continue // another orchestrator instance won
		}
		d.fireStageWebhooks(ctx, stage)
	}

	// 3+4. Finalize backstop and timeout sweep over running stages.
	running, err := d.stageRepo.ListRunning(50)
	if err != nil {
		logger.Warnf("task stages: list running: %v", err)
		return
	}
	now := time.Now()
	for i := range running {
		stage := &running[i]

		timedOut := false
		if stage.RunningAt != nil && now.Sub(*stage.RunningAt) > d.maxDuration {
			timedOut = true
		} else if stage.LastProgressAt != nil && now.Sub(*stage.LastProgressAt) > d.progressTimeout {
			timedOut = true
		}
		if timedOut {
			msg := fmt.Sprintf("timed out: no result within %s (progress) / %s (total)", d.progressTimeout, d.maxDuration)
			if err := d.resultRepo.TimeoutNonTerminal(stage.ID, msg); err != nil {
				logger.Warnf("task stages: timeout results of %s: %v", stage.ID, err)
				continue
			}
			// Reload so the engine folds the errored results.
			if reloaded, err := d.stageRepo.GetByID(stage.ID); err == nil {
				stage = reloaded
			}
			logger.Infof("task stage %s timed out; folding verdicts", stage.ID)
		}

		if err := d.engine.FinalizeStage(stage); err != nil {
			logger.Warnf("task stages: finalize %s: %v", stage.ID, err)
		}
	}
}

// fireStageWebhooks POSTs one signed request per task result of a freshly claimed stage. Delivery
// failure marks that result `unreachable` (TFE semantics); the engine folds verdicts as callbacks
// arrive (or immediately, if every delivery failed).
func (d *taskStageDeps) fireStageWebhooks(ctx context.Context, stage *models.TaskStage) {
	run, err := d.runRepo.GetByID(stage.RunID)
	if err != nil {
		logger.Warnf("task stages: load run %s: %v", stage.RunID, err)
		return
	}
	org, err := d.orgRepo.GetByID(run.Workspace.Project.OrganizationID)
	if err != nil {
		logger.Warnf("task stages: load org for run %s: %v", stage.RunID, err)
		return
	}

	// Shared run/workspace payload fields.
	base := runtask.Request{
		PayloadVersion:            1,
		Stage:                     stage.Stage,
		Capabilities:              runtask.Capabilities{Outcomes: true},
		OrganizationName:          org.Name,
		IsSpeculative:             false,
		RunAppURL:                 fmt.Sprintf("%s/app/%s/workspaces/%s/runs/%s", d.appURL, org.Name, run.Workspace.Name, run.ID),
		RunCreatedAt:              run.CreatedAt.Format(time.RFC3339),
		RunID:                     run.ID,
		WorkspaceAppURL:           fmt.Sprintf("%s/app/%s/workspaces/%s", d.appURL, org.Name, run.Workspace.Name),
		WorkspaceID:               run.WorkspaceID,
		WorkspaceName:             run.Workspace.Name,
		WorkspaceWorkingDirectory: run.Workspace.WorkingDirectory,
	}
	if run.CreatedBy != nil {
		if u, err := d.userRepo.GetByID(*run.CreatedBy); err == nil {
			base.RunCreatedBy = u.Email
		}
	}
	if run.ConfigurationVersionID != nil {
		cvID := *run.ConfigurationVersionID
		base.ConfigurationVersionID = &cvID
		dl := fmt.Sprintf("%s/api/v2/configuration-versions/%s/download", d.apiURL, cvID)
		base.ConfigurationVersionDownloadURL = &dl
		if cv, err := d.configVerRepo.GetByID(cvID); err == nil && cv != nil {
			base.IsSpeculative = cv.Speculative
			if cv.SourceBranch != "" {
				b := cv.SourceBranch
				base.VCSBranch = &b
			}
			if cv.CommitHash != "" {
				h := cv.CommitHash
				base.VCSCommitHash = &h
			}
		}
	}
	if stage.Stage != models.TaskStagePrePlan {
		planURL := fmt.Sprintf("%s/api/v2/plans/%s/json-output", d.apiURL, run.ID)
		base.PlanJSONAPIURL = &planURL
	}

	for i := range stage.TaskResults {
		result := &stage.TaskResults[i]
		token := terraformHandlers.MintTaskResultToken(result.ID, run.ID)
		if token == "" {
			logger.Warnf("task stages: cannot mint access token for %s (no ENCRYPTION_KEY?); marking unreachable", result.ID)
			_, _ = d.resultRepo.SetStatus(result.ID, []string{models.TaskResultStatusPending}, models.TaskResultStatusUnreachable,
				"platform could not mint an access token", "")
			continue
		}
		payload := base
		payload.AccessToken = token
		payload.TaskResultID = result.ID
		payload.TaskResultEnforcementLevel = result.EnforcementLevel
		payload.TaskResultCallbackURL = fmt.Sprintf("%s/api/v2/task-results/%s/callback", d.apiURL, result.ID)

		if err := d.taskSvc.Deliver(ctx, result.TaskURL, result.HMACKeyEncrypted, payload); err != nil {
			logger.Warnf("task stages: deliver %s to %s failed: %v", result.ID, result.TaskURL, err)
			_, _ = d.resultRepo.SetStatus(result.ID, []string{models.TaskResultStatusPending}, models.TaskResultStatusUnreachable,
				fmt.Sprintf("delivery failed: %v", err), "")
			continue
		}
		logger.Infof("task webhook delivered: result %s (%s, %s) for run %s", result.ID, result.TaskName, stage.Stage, run.ID)
	}

	// If every delivery failed, fold immediately instead of waiting for the sweep.
	if reloaded, err := d.stageRepo.GetByID(stage.ID); err == nil {
		if err := d.engine.FinalizeStage(reloaded); err != nil {
			logger.Warnf("task stages: finalize after delivery for %s: %v", stage.ID, err)
		}
	}
}
