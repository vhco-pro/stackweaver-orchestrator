// Copyright (c) 2025 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/michielvha/logger"
	terraformHandlers "github.com/michielvha/stackweaver/backend/internal/api/v2/handlers/terraform"
	"github.com/michielvha/stackweaver/backend/internal/services/rbac"
	"github.com/michielvha/stackweaver/core/crypto"
	"github.com/michielvha/stackweaver/core/models"
	"github.com/michielvha/stackweaver/core/queue"
	"github.com/michielvha/stackweaver/core/repository"
	"github.com/michielvha/stackweaver/core/services/ansible"
	"github.com/michielvha/stackweaver/core/services/encryptionkey"
	"github.com/michielvha/stackweaver/core/services/notification"
	"github.com/michielvha/stackweaver/core/services/runtask"
	"github.com/michielvha/stackweaver/core/services/vcs"
	"github.com/michielvha/stackweaver/core/services/wakeup"
	"github.com/michielvha/stackweaver/core/settings"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// getEnv returns the value of an environment variable or a fallback default.
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// getEnvInt returns the integer value of an environment variable or a fallback default.
func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func main() {
	// Initialize logger first (reads LOG_LEVEL from environment)
	logLevel := os.Getenv("LOG_LEVEL")
	logger.Init(logLevel)

	// Initialize queue
	redisQueue, err := queue.NewRedisQueue(
		getEnv("REDIS_HOST", "localhost"),
		getEnvInt("REDIS_PORT", 6379),
		os.Getenv("REDIS_PASSWORD"),
		getEnvInt("REDIS_DB", 0),
	)
	if err != nil {
		logger.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer func() {
		if err := redisQueue.Close(); err != nil {
			logger.Warnf("Failed to close Redis queue: %v", err)
		}
	}()

	// Initialize database
	db, err := repository.NewDatabase(repository.Config{
		Host:            getEnv("DATABASE_HOST", "localhost"),
		Port:            getEnvInt("DATABASE_PORT", 5432),
		User:            getEnv("DATABASE_USER", "iac"),
		Password:        getEnv("DATABASE_PASSWORD", "iac_password"),
		DBName:          getEnv("DATABASE_NAME", "iac_platform"),
		SSLMode:         getEnv("DATABASE_SSLMODE", "disable"),
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	})
	if err != nil {
		// Close Redis queue before exiting
		if closeErr := redisQueue.Close(); closeErr != nil {
			logger.Warnf("Failed to close Redis queue before exit: %v", closeErr)
		}
		//nolint:gocritic // False positive: we explicitly close redisQueue before logger.Fatalf
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	// The plan JSON runs store is sealed at rest under ENCRYPTION_KEY (#933, D2, AC73), the key the
	// API seals it with; the orchestrator reads it for commit statuses and run-task stages.
	planSealer, err := crypto.NewCryptoService(encryptionkey.Resolve(os.Getenv("ENCRYPTION_KEY")))
	if err != nil {
		logger.Fatalf("Setting up encryption at rest of plan output: %v", err)
	}
	models.SetPlanOutputSealer(planSealer)
	// A run the orchestrator creates (a run trigger's) wakes its pool's launchers once committed
	// (#933, AC19); the routing sweep below stays the safety net for a lost wakeup.
	repository.SetRunWaker(wakeup.PoolWaker(redisQueue.Client()))

	runRepo := repository.NewRunRepository(db)
	workspaceRepo := repository.NewWorkspaceRepository(db)
	configVersionRepo := repository.NewConfigurationVersionRepository(db)
	runTriggerRepo := repository.NewRunTriggerRepository(db)
	vcsConnectionRepo := repository.NewVCSConnectionRepository(db)
	ansibleJobRepo := repository.NewAnsibleJobRepository(db)

	// Initialize GitHub App Manager and status service for PR status checks
	var statusService *vcs.GitHubStatusService
	githubAppManager, err := vcs.NewGitHubAppManager()
	if err != nil {
		logger.Warnf("Failed to initialize GitHub App Manager: %v (GitHub PR status checks will be disabled)", err)
	} else if githubAppManager != nil && githubAppManager.IsEnabled() {
		appService := githubAppManager.GetService()
		if appService != nil {
			statusService = vcs.NewGitHubStatusService(appService)
			logger.Info("GitHub status check service initialized for PR status checks")
		}
	}

	// Initialize Azure DevOps Manager and status service for PR status checks
	var adoStatusService *vcs.AzureDevOpsStatusService
	azureDevOpsManager, adoErr := vcs.NewAzureDevOpsManager()
	if adoErr != nil {
		logger.Warnf("Failed to initialize Azure DevOps Manager: %v (ADO PR status checks will be disabled)", adoErr)
	} else if azureDevOpsManager != nil && azureDevOpsManager.IsEnabled() {
		// Create a connUpdater that persists token changes to the database
		connUpdater := func(conn *models.VCSConnection) error {
			return vcsConnectionRepo.Update(conn)
		}
		adoStatusService = vcs.NewAzureDevOpsStatusService(azureDevOpsManager, connUpdater)
		logger.Info("Azure DevOps status check service initialized for PR status checks")
	}

	// Resolve the encryption-at-rest key so the registry can decrypt VCS connection
	// tokens at rest (#95) and re-encrypt refreshed tokens. Unlike the api/runner -
	// which require the key for variables/state/credentials and so fail loud (AUD-013)
	// - the orchestrator only needs it for the optional VCS path, so resolution here is
	// best-effort: a missing/invalid key logs a warning and leaves crypto nil (tokens
	// treated as plaintext, exactly as before #95), never crashing the scheduler.
	var atRestCrypto *crypto.CryptoService
	if keyBytes, keyErr := crypto.DeriveKey(os.Getenv("ENCRYPTION_KEY")); keyErr != nil {
		logger.Warnf("VCS token decryption disabled (ENCRYPTION_KEY %v); set it to match the API to enable encrypted VCS connections", keyErr)
	} else if cs, csErr := crypto.NewCryptoService(keyBytes); csErr != nil {
		logger.Warnf("VCS token decryption disabled: %v", csErr)
	} else {
		atRestCrypto = cs
	}

	// Create a provider registry for ADO token refresh
	vcsRegistry := vcs.NewProviderRegistry(githubAppManager, azureDevOpsManager, func(conn *models.VCSConnection) error {
		return vcsConnectionRepo.Update(conn)
	}, atRestCrypto)

	// Workspace run-notification delivery (tfe_notification_configuration). The HMAC token is decrypted
	// with the SAME key derivation the API used to encrypt it (encryptionkey.Resolve), not the
	// orchestrator's VCS crypto above (which uses a different, fail-open derivation and can diverge from
	// the API's key, and that mismatch would silently deliver unsigned webhooks).
	var notifCrypto *crypto.CryptoService
	if kb := encryptionkey.Resolve(os.Getenv("ENCRYPTION_KEY")); len(kb) > 0 {
		notifCrypto, _ = crypto.NewCryptoService(kb)
	}
	notificationSvc := notification.NewService(repository.NewNotificationConfigurationRepository(db), notifCrypto, os.Getenv("STACKWEAVER_APP_URL"))

	// change_request:created delivery (tfe_team_notification_configuration). The audience is every team
	// that can reach the change request's workspace, which is an rbac question (a union of direct,
	// project, org-wide and owners access), so it is resolved here and passed into the notifier: core
	// must not import backend.
	changeRequestRepo := repository.NewChangeRequestRepository(db)
	changeRequestUserRepo := repository.NewUserRepository(db)
	changeRequestRBAC := rbac.NewServiceWithTeams(
		repository.NewOrganizationRepository(db),
		repository.NewTeamRepository(db),
		repository.NewProjectRepository(db),
	)

	// Run-task stage driver (tfe_organization_run_task family): fires signed stage webhooks, sweeps
	// timeouts, and continues gated runs. Uses the SAME encryptionkey-resolved crypto as the API
	// (see the notification comment above - the fail-open VCS derivation must not sign webhooks),
	// both for decrypting snapshot HMAC keys and for minting the callback access tokens.
	terraformHandlers.SetTaskTokenSecret(encryptionkey.Resolve(os.Getenv("ENCRYPTION_KEY")))
	taskStageRepo := repository.NewTaskStageRepository(db)
	taskResultRepo := repository.NewTaskResultRepository(db)
	taskProgress, taskCap := taskTimeouts()
	taskDeps := &taskStageDeps{
		runRepo:         runRepo,
		stageRepo:       taskStageRepo,
		resultRepo:      taskResultRepo,
		configVerRepo:   configVersionRepo,
		orgRepo:         repository.NewOrganizationRepository(db),
		userRepo:        changeRequestUserRepo,
		taskSvc:         runtask.NewService(notifCrypto),
		engine:          runtask.NewEngine(runRepo, taskStageRepo),
		appURL:          os.Getenv("STACKWEAVER_APP_URL"),
		apiURL:          os.Getenv("STACKWEAVER_API_URL"),
		progressTimeout: taskProgress,
		maxDuration:     taskCap,
	}

	// Start orchestrator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// AUD-132: track the worker goroutines so SIGTERM waits for their current iteration to finish
	// (drain) instead of returning immediately and killing in-flight DB writes mid-statement.
	var wg sync.WaitGroup

	// The API migrates the schema; on an upgrade this process can start first. Nothing below runs
	// against the previous release's schema, and a stop signal still ends the wait.
	waitCtx, stopWait := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	err = waitForSchema(waitCtx, func() ([]string, error) { return models.MissingSchema(db) }, schemaWaitInterval, logger.Warnf)
	stopWait()
	if err != nil {
		logger.Info("Orchestrator stopped while waiting for the database schema")
		return
	}

	// Runs a retired queue runner was handed are offered to launchers (#933, D14).
	if err := sweepQueueDispatches(runRepo, 0); err != nil {
		logger.Errorf("Error sweeping queue dispatches at start: %v", err)
	}
	// Ansible jobs a release before the launchers queued are given their hosted pool (#933, D9).
	if err := sweepPoollessJobs(ctx, db, redisQueue.Client()); err != nil {
		logger.Errorf("Error sweeping pool-less Ansible jobs at start: %v", err)
	}

	// Process pending runs every 5 seconds
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - processing pending runs")
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := processPendingRuns(ctx, db, redisQueue, runRepo); err != nil {
					logger.Errorf("Error processing pending runs: %v", err)
				}
			}
		}
	}()

	// Clean up stuck runs and stuck ansible jobs every 1 minute
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - cleaning up stuck runs and ansible jobs")
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := cleanupStuckRuns(ctx, runRepo); err != nil {
					logger.Errorf("Error cleaning up stuck runs: %v", err)
				}
				if err := cleanupStuckAnsibleJobs(ctx, ansibleJobRepo); err != nil {
					logger.Errorf("Error cleaning up stuck ansible jobs: %v", err)
				}
				if err := sweepQueueDispatches(runRepo, 5*time.Minute); err != nil {
					logger.Errorf("Error sweeping queue dispatches: %v", err)
				}
				if err := sweepPoollessJobs(ctx, db, redisQueue.Client()); err != nil {
					logger.Errorf("Error sweeping pool-less Ansible jobs: %v", err)
				}
			}
		}
	}()

	// Update PR status checks every 10 seconds
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - updating PR status checks")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := updatePRStatusChecks(ctx, runRepo, workspaceRepo, configVersionRepo, vcsConnectionRepo, statusService, adoStatusService, vcsRegistry); err != nil {
					logger.Errorf("Error updating PR status checks: %v", err)
				}
			}
		}
	}()

	// Deliver workspace run-notifications (tfe_notification_configuration) every 10 seconds. Fires when a
	// run's status advances past the last-notified status; independent of which runner executed it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - delivering run notifications")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processRunNotifications(ctx, runRepo, notificationSvc)
			}
		}
	}()

	// Deliver change-request notifications (tfe_team_notification_configuration) every 10 seconds. Fires
	// once per change request, to the configs of every team that can reach its workspace.
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - delivering change request notifications")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processChangeRequestNotifications(ctx, changeRequestRepo, changeRequestRBAC, changeRequestUserRepo, notificationSvc)
			}
		}
	}()

	// Drive run-task stages every 5 seconds: open pre_plan gates, fire freshly claimed stages'
	// webhooks, finalize completed stages (callback backstop), and sweep timeouts. 5s (not 10s)
	// because a stage boundary sits INSIDE the run's critical path - every tick of latency here is
	// wall-clock added to every gated run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - driving run-task stages")
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processTaskStages(ctx, taskDeps)
			}
		}
	}()

	// Fire run triggers (tfe_run_trigger) every 10 seconds: when a run applies, queue a run in each
	// downstream target workspace. Runs in the orchestrator so it is independent of which runner
	// (remote or agent) applied the source run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Orchestrator started - firing run triggers")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := processRunTriggers(ctx, runRepo, runTriggerRepo, workspaceRepo, configVersionRepo); err != nil {
					logger.Errorf("Error firing run triggers: %v", err)
				}
			}
		}
	}()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// AUD-132: graceful shutdown - cancel the context and wait (bounded) for the workers to
	// finish their current iteration so we don't abandon in-flight DB writes.
	logger.Info("Shutting down orchestrator...")
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		logger.Info("Orchestrator workers drained; exiting")
	case <-time.After(10 * time.Second):
		logger.Warn("Timed out waiting for orchestrator workers to drain; exiting")
	}
}

func processPendingRuns(ctx context.Context, db *gorm.DB, redisQueue *queue.RedisQueue, runRepo *repository.RunRepository) error {
	// Get pending runs (initial state) and applying runs (plan-and-apply runs ready for apply phase)
	pendingRuns, err := runRepo.ListByStatus(models.RunStatusPending, 10)
	if err != nil {
		return fmt.Errorf("failed to list pending runs: %w", err)
	}

	// Runs whose pre_plan task stage passed are dispatchable exactly like pending. (Pending runs
	// with an UNPASSED pre_plan stage may appear in the list above, but ClaimForDispatch refuses
	// them - the gate lives in the claim, shared with the agent job-start path.)
	gatePassedRuns, err := runRepo.ListByStatus(models.RunStatusPrePlanCompleted, 10)
	if err != nil {
		return fmt.Errorf("failed to list pre_plan_completed runs: %w", err)
	}

	// Also get runs in "applying" status (plan-and-apply runs ready for apply phase)
	applyingRuns, err := runRepo.ListByStatus(models.RunStatusApplying, 10)
	if err != nil {
		return fmt.Errorf("failed to list applying runs: %w", err)
	}

	// Combine the lists
	runs := make([]models.Run, 0, len(pendingRuns)+len(gatePassedRuns)+len(applyingRuns))
	runs = append(runs, pendingRuns...)
	runs = append(runs, gatePassedRuns...)
	runs = append(runs, applyingRuns...)

	for _, run := range runs {
		// Skip runs that are too old - abandoned ones are handled by cleanupStuckRuns. AUD-110:
		// the age must be measured from the CURRENT phase's start, not from run creation. A
		// plan-and-apply run that a human reviews for 30+ minutes before clicking "Confirm & Apply"
		// has an old CreatedAt but a freshly-confirmed apply; keying the skip on CreatedAt wedged
		// it in `applying` forever (the completely normal review-then-apply flow). For applying
		// runs use ApplyStartedAt (nil until a runner picks it up → age 0 → enqueue); for pending
		// runs keep CreatedAt.
		var age time.Duration
		if run.Status == models.RunStatusApplying {
			if run.ApplyStartedAt != nil {
				age = time.Since(*run.ApplyStartedAt)
			}
		} else {
			age = time.Since(run.CreatedAt)
		}
		if age > 30*time.Minute {
			logger.Infof("Skipping enqueue for old run %s (status=%s, phase age %s), it should be cleaned up by the stuck run cleaner.", run.ID, run.Status, age.Round(time.Second))
			continue
		}

		// Reload run to check if status changed (might have been cancelled or failed)
		// This prevents enqueueing runs that were cancelled/failed between the query and now
		reloadedRun, err := runRepo.GetByID(run.ID)
		if err != nil {
			logger.Errorf("Failed to reload run %s: %v", run.ID, err)
			continue
		}

		// Skip if run is no longer dispatchable (might have been cancelled, failed, or started).
		// "applying" status means plan-and-apply run is ready for apply phase;
		// "pre_plan_completed" means its pre_plan task stage passed and the plan can dispatch.
		if reloadedRun.Status != models.RunStatusPending && reloadedRun.Status != models.RunStatusApplying &&
			reloadedRun.Status != models.RunStatusPrePlanCompleted {
			logger.Infof("Skipping enqueue for run %s - status changed to %s", reloadedRun.ID, reloadedRun.Status)
			continue
		}

		// Route the run (#933, D9): the customer pool the level rule resolves, or the
		// organization's hosted pool when everything resolves to remote. Nothing is enqueued: a
		// launcher of the pool is offered the run through its heartbeat, and one holding a long
		// poll is woken for it. Only an unreserved run is re-stamped, so routing never moves a run
		// a launcher already holds.
		if err := routeRun(ctx, db, redisQueue.Client(), reloadedRun); err != nil {
			logger.Errorf("Failed to route run %s: %v", reloadedRun.ID, err)
		}
		// A VCS-backed run without a configuration version waits for the configuration_snapshot
		// work item that captures it (#933, decision 18); the executor never clones.
		if err := requestRunConfiguration(ctx, db, redisQueue.Client(), reloadedRun); err != nil {
			logger.Errorf("Failed to request the configuration of run %s: %v", reloadedRun.ID, err)
		}
	}

	return nil
}

// requestRunConfiguration creates the configuration_snapshot work item of a run waiting for its
// plan whose workspace is VCS-backed and that has no configuration version; one that exists is
// left alone.
func requestRunConfiguration(ctx context.Context, db *gorm.DB, rdb *redis.Client, run *models.Run) error {
	if run.ConfigurationVersionID != nil || run.Status == models.RunStatusApplying {
		return nil
	}
	var ws models.Workspace
	if err := db.Select("id", "vcs_connection_id", "vcs_repository").First(&ws, "id = ?", run.WorkspaceID).Error; err != nil {
		return fmt.Errorf("loading workspace %s: %w", run.WorkspaceID, err)
	}
	if ws.VCSConnectionID == nil || ws.VCSRepository == "" {
		return nil
	}
	_, err := ansible.NewWorkItemService(db, rdb).RequestConfigurationSnapshot(ctx, run)
	return err
}

// processRunTriggers fires tfe_run_trigger links: for each newly-applied run that hasn't been
// processed yet, it queues a plan-and-apply run in every downstream target workspace whose trigger
// names the applied run's workspace as the source. The triggered run is created as `pending`; the
// pending-run worker then dispatches it like any other run.
//
// The applied source run is atomically claimed (run_triggers_fired_at) before any downstream run is
// created, so with multiple orchestrator instances each source fires exactly once.
func processRunTriggers(
	ctx context.Context,
	runRepo *repository.RunRepository,
	runTriggerRepo *repository.RunTriggerRepository,
	workspaceRepo *repository.WorkspaceRepository,
	configVersionRepo *repository.ConfigurationVersionRepository,
) error {
	applied, err := runRepo.ListAppliedWithUnfiredTriggers(50)
	if err != nil {
		return fmt.Errorf("failed to list applied runs with unfired triggers: %w", err)
	}
	for i := range applied {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		src := applied[i]

		// Claim first so a source fires once even across orchestrator instances / overlapping ticks.
		won, err := runRepo.ClaimTriggersFired(src.ID)
		if err != nil {
			logger.Errorf("run-trigger: failed to claim run %s: %v", src.ID, err)
			continue
		}
		if !won {
			continue // another worker owns this source's firing
		}

		triggers, err := runTriggerRepo.ListBySourceable(src.WorkspaceID)
		if err != nil {
			logger.Errorf("run-trigger: failed to list triggers for source workspace %s: %v", src.WorkspaceID, err)
			continue
		}
		if len(triggers) == 0 {
			continue
		}

		for _, trig := range triggers {
			target, err := workspaceRepo.GetByID(trig.WorkspaceID)
			if err != nil {
				logger.Warnf("run-trigger %s: target workspace %s not found, skipping: %v", trig.ID, trig.WorkspaceID, err)
				continue
			}
			// The target needs a configuration to run. TFE queues against the target's latest config.
			latestCV, err := configVersionRepo.GetLatestByWorkspaceID(target.ID)
			if err != nil || latestCV == nil {
				logger.Warnf("run-trigger %s: target workspace %s has no configuration version, skipping downstream run", trig.ID, target.ID)
				continue
			}
			cvID := latestCV.ID
			newRun := &models.Run{
				WorkspaceID:            target.ID,
				ConfigurationVersionID: &cvID,
				CreatedBy:              src.CreatedBy, // carry the source run's actor for audit/RBAC
				Status:                 models.RunStatusPending,
				Operation:              models.RunOperationPlanAndApply,
				AutoApplyAfterPlan:     target.AutoApply, // respect the target workspace's auto-apply
				AgentPoolID:            target.AgentPoolID,
			}
			if err := runRepo.Create(newRun); err != nil {
				logger.Errorf("run-trigger %s: failed to queue run in target %s: %v", trig.ID, target.ID, err)
				continue
			}
			logger.Infof("run-trigger %s: source %s applied → queued run %s in target workspace %s",
				trig.ID, src.ID, newRun.ID, target.ID)
		}
	}
	return nil
}

// cleanupStuckRuns finds and marks stuck runs as failed
// - Running runs that have exceeded their timeout (workspace RunTimeout, default 1 hour)
// - Pending runs that have been pending for more than 30 minutes (truly abandoned)
// Note: We only clean up runs that have exceeded their actual timeout - normal runs can take 30+ minutes
// processRunNotifications delivers workspace notifications for runs whose status advanced past the
// last-notified status, then advances the marker. Best-effort: delivery failures are logged inside the
// notifier and do not stop the marker from advancing (avoids retry storms on a broken endpoint).
func processRunNotifications(ctx context.Context, runRepo *repository.RunRepository, notifier *notification.Service) {
	runs, err := runRepo.ListRunsNeedingNotification(50)
	if err != nil {
		logger.Warnf("notifications: list runs needing notification: %v", err)
		return
	}
	for i := range runs {
		run := &runs[i]
		notifier.NotifyRun(ctx, notification.RunContext{
			RunID:         run.ID,
			WorkspaceID:   run.WorkspaceID,
			ProjectID:     run.Workspace.ProjectID,
			WorkspaceName: run.Workspace.Name,
			Organization:  run.Workspace.Project.Organization.Name,
			Status:        run.Status,
			Operation:     run.Operation,
			Message:       run.ErrorMessage,
			UpdatedAt:     run.UpdatedAt,
		})
		if err := runRepo.MarkNotified(run.ID, run.Status); err != nil {
			logger.Warnf("notifications: mark run %s notified: %v", run.ID, err)
		}
	}
}

// processChangeRequestNotifications delivers change_request:created to the notification configs of every
// team that can reach each new change request's workspace, then stamps the marker. Same best-effort
// contract as processRunNotifications: the marker advances even when delivery fails, so one broken
// endpoint cannot cause a retry storm, at the cost of at-most-once delivery.
func processChangeRequestNotifications(
	ctx context.Context,
	crRepo *repository.ChangeRequestRepository,
	rbacSvc *rbac.Service,
	userRepo *repository.UserRepository,
	notifier *notification.Service,
) {
	crs, err := crRepo.ListNeedingNotification(50)
	if err != nil {
		logger.Warnf("notifications: list change requests needing notification: %v", err)
		return
	}
	for i := range crs {
		cr := &crs[i]
		if cr.Workspace == nil {
			logger.Warnf("notifications: change request %s has no workspace loaded; skipping", cr.ID)
			continue
		}

		teams, terr := rbacSvc.ListTeamsWithWorkspaceAccess(
			cr.Workspace.Project.OrganizationID, cr.Workspace.ProjectID, cr.WorkspaceID)
		if terr != nil {
			// Do NOT advance the marker: this is a lookup failure, not a delivery failure, so the
			// request has not had its chance to notify yet and should be retried next tick.
			logger.Warnf("notifications: resolve teams for change request %s: %v", cr.ID, terr)
			continue
		}

		teamIDs := make([]uuid.UUID, 0, len(teams))
		for _, t := range teams {
			teamIDs = append(teamIDs, t.TeamID)
		}

		// TFE puts the filer's email in change_request_created_by.
		createdBy := cr.CreatedBy.String()
		if u, uerr := userRepo.GetByID(cr.CreatedBy); uerr == nil && u.Email != "" {
			createdBy = u.Email
		}

		notifier.NotifyChangeRequest(ctx, notification.ChangeRequestContext{
			ChangeRequestID: cr.ID,
			Subject:         cr.Subject,
			Message:         cr.Message,
			CreatedBy:       createdBy,
			CreatedAt:       cr.CreatedAt,
			WorkspaceID:     cr.WorkspaceID,
			WorkspaceName:   cr.Workspace.Name,
			Organization:    cr.Workspace.Project.Organization.Name,
		}, teamIDs)

		if err := crRepo.MarkNotified(cr.ID); err != nil {
			logger.Warnf("notifications: mark change request %s notified: %v", cr.ID, err)
		}
	}
}

func cleanupStuckRuns(ctx context.Context, runRepo *repository.RunRepository) error {
	// Find stuck runs (pending for more than 30 minutes, or running longer than their run timeout,
	// resolved by the level rule down to the deployment default).
	deploymentTimeout := settings.DeploymentRunTimeout(os.Getenv)
	stuckRuns, err := runRepo.FindStuckRunsWithDefault(30*time.Minute, time.Duration(deploymentTimeout)*time.Second)
	if err != nil {
		return fmt.Errorf("failed to find stuck runs: %w", err)
	}

	if len(stuckRuns) == 0 {
		return nil
	}

	logger.Infof("Found %d stuck run(s), marking as failed", len(stuckRuns))

	for _, run := range stuckRuns {
		var errorMessage string
		if run.Status == models.RunStatusPending {
			// Pending run that was abandoned (more than 30 minutes)
			errorMessage = "Run was pending for too long and was automatically cancelled"
		} else {
			// Actively-executing run (planning/applying/legacy running) that exceeded its timeout.
			timeout := settings.EffectiveWorkspaceRunTimeout(&run.Workspace, deploymentTimeout).Value
			errorMessage = fmt.Sprintf("Run exceeded timeout of %d seconds and was automatically cancelled", timeout)
		}

		// MarkAsFailed is guarded (AUD-132) - it no-ops if the run already reached a terminal
		// state between the sweep query and now, so it cannot ping-pong a finished run to failed.
		if err := runRepo.MarkAsFailed(run.ID, errorMessage); err != nil {
			logger.Errorf("Failed to mark run %s as failed: %v", run.ID, err)
			continue
		}

		logger.Infof("Marked stuck run %s as failed: %s", run.ID, errorMessage)
	}

	return nil
}

// sweepQueueDispatches re-dispatches runs a retired queue runner was handed and never started
// (#933, D14): at start every one, then those claimed five minutes ago or more, so a run an old
// orchestrator enqueued during a rolling upgrade is offered to a launcher instead of stranded.
func sweepQueueDispatches(runRepo *repository.RunRepository, olderThan time.Duration) error {
	n, err := runRepo.ReleaseQueueDispatches(olderThan)
	if err != nil {
		return fmt.Errorf("releasing queue dispatches: %w", err)
	}
	if n > 0 {
		logger.Infof("Re-dispatched %d run(s) claimed onto a retired queue", n)
	}
	return nil
}

// poollessJobsPerSweep bounds the jobs one sweep stamps.
const poollessJobsPerSweep = 200

// sweepPoollessJobs gives a pending Ansible job that carries no pool its organization's hosted pool
// and wakes that pool's launchers (#933, D9, AC58). Only a launch stamps a pool, so a job a release
// before the launchers queued would otherwise never be offered after an upgrade. rdb may be nil.
func sweepPoollessJobs(ctx context.Context, db *gorm.DB, rdb *redis.Client) error {
	var stray []struct {
		ID             uuid.UUID
		OrganizationID uuid.UUID
	}
	err := db.WithContext(ctx).Table("ansible_jobs").
		Select("ansible_jobs.id, projects.organization_id").
		Joins("JOIN projects ON projects.id = ansible_jobs.project_id").
		Where("ansible_jobs.status = ? AND ansible_jobs.agent_pool_id IS NULL", models.AnsibleJobStatusPending).
		Limit(poollessJobsPerSweep).Scan(&stray).Error
	if err != nil {
		return fmt.Errorf("listing pending jobs without a pool: %w", err)
	}
	woken := map[uuid.UUID]bool{}
	for _, j := range stray {
		pool, _, err := repository.HostedPoolOf(db, j.OrganizationID)
		if err != nil {
			return err
		}
		if err := db.WithContext(ctx).Model(&models.AnsibleJob{}).
			Where("id = ? AND agent_pool_id IS NULL", j.ID).Update("agent_pool_id", pool).Error; err != nil {
			return fmt.Errorf("stamping job %s with its hosted pool: %w", j.ID, err)
		}
		if rdb != nil && !woken[pool] {
			wakeup.Publish(ctx, rdb, wakeup.KeyForPool(pool.String(), true))
			woken[pool] = true
		}
	}
	if len(stray) > 0 {
		logger.Infof("Gave %d pending Ansible job(s) without a pool their hosted pool", len(stray))
	}
	return nil
}

// cleanupStuckAnsibleJobs recovers Ansible jobs whose executor died mid-run, leaving them stuck
// in `running` forever (AUD-016 - Ansible previously had no stuck-job recovery at all). A job's
// TimeoutSeconds is resolved by the level rule at launch; the deployment default covers a job
// launched before that, which carries none.
func cleanupStuckAnsibleJobs(ctx context.Context, jobRepo *repository.AnsibleJobRepository) error {
	stuckJobs, err := jobRepo.FindStuckJobs(time.Duration(settings.DeploymentRunTimeout(os.Getenv)) * time.Second)
	if err != nil {
		return fmt.Errorf("failed to find stuck ansible jobs: %w", err)
	}
	if len(stuckJobs) == 0 {
		return nil
	}

	logger.Infof("Found %d stuck ansible job(s), marking as failed", len(stuckJobs))
	for _, job := range stuckJobs {
		msg := "Ansible job exceeded its timeout and was automatically failed (runner presumed dead)"
		ok, failErr := jobRepo.FailIfRunning(job.ID, msg)
		if failErr != nil {
			logger.Errorf("Failed to mark stuck ansible job %s as failed: %v", job.ID.String(), failErr)
			continue
		}
		if ok {
			logger.Infof("Marked stuck ansible job %s as failed", job.ID.String())
		}
	}
	return nil
}

// updatePRStatusChecks polls for PR runs and updates their VCS status checks (GitHub and Azure DevOps)
func updatePRStatusChecks(
	ctx context.Context,
	runRepo *repository.RunRepository,
	workspaceRepo *repository.WorkspaceRepository,
	configVersionRepo *repository.ConfigurationVersionRepository,
	vcsConnectionRepo *repository.VCSConnectionRepository,
	statusService *vcs.GitHubStatusService,
	adoStatusService *vcs.AzureDevOpsStatusService,
	vcsRegistry *vcs.ProviderRegistry,
) error {
	// Skip if no status service is available at all
	if statusService == nil && adoStatusService == nil {
		return nil
	}

	// Get PR runs that need status check updates (speculative runs with active statuses)
	runs, err := runRepo.ListPRRunsForStatusChecks(50)
	if err != nil {
		return fmt.Errorf("failed to list PR runs: %w", err)
	}

	if len(runs) == 0 {
		return nil
	}

	logger.Infof("[STATUS_CHECK] Found %d PR run(s) to check for status check updates", len(runs))

	for i := range runs {
		updatePRStatusCheck(ctx, &runs[i], runRepo, workspaceRepo, configVersionRepo, vcsConnectionRepo, statusService, adoStatusService, vcsRegistry)
	}

	return nil
}

// sync smoke test 1779753492

// routeRun stamps a waiting run with the pool it executes on and wakes that pool's launchers.
func routeRun(ctx context.Context, db *gorm.DB, rdb *redis.Client, run *models.Run) error {
	pool, hosted, err := repository.RunPool(db, run.WorkspaceID)
	if err != nil {
		return err
	}
	if run.AgentPoolID == nil || *run.AgentPoolID != pool {
		res := db.Model(&models.Run{}).Where("id = ? AND runner_id IS NULL", run.ID).Update("agent_pool_id", pool)
		if res.Error != nil {
			return fmt.Errorf("stamping run %s with pool %s: %w", run.ID, pool, res.Error)
		}
		if res.RowsAffected > 0 {
			logger.Infof("Run %s routed to pool %s (hosted %t)", run.ID, pool, hosted)
		}
	}
	if run.RunnerID == nil {
		wakeup.Publish(ctx, rdb, wakeup.KeyForPool(pool.String(), hosted))
	}
	return nil
}
