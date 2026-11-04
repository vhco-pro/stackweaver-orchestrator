// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// On an upgrade the orchestrator can start before the API has migrated the database. It waits,
// saying what is missing, and its start-time sweeps run only once the schema is this release's.
func TestWaitForSchema_WaitsUntilTheMigrationHasRun(t *testing.T) {
	calls := 0
	check := func() ([]string, error) {
		calls++
		switch calls {
		case 1:
			return nil, errors.New("connection refused")
		case 2:
			return []string{"agent_pools.kind", "work_items"}, nil
		}
		return nil, nil
	}
	var logs []string
	logf := func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }

	if err := waitForSchema(context.Background(), check, time.Millisecond, logf); err != nil {
		t.Fatalf("waitForSchema: %v", err)
	}
	if calls != 3 {
		t.Fatalf("checked %d times, want 3", calls)
	}
	joined := strings.Join(logs, "\n")
	for _, want := range []string{"connection refused", "agent_pools.kind"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the wait did not log %q: %s", want, joined)
		}
	}
}

// A schema that is already this release's costs one check and no wait.
func TestWaitForSchema_ReadySchemaReturnsAtOnce(t *testing.T) {
	calls := 0
	if err := waitForSchema(context.Background(), func() ([]string, error) { calls++; return nil, nil }, time.Hour, func(string, ...any) {}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("checked %d times, want 1", calls)
	}
}

// A stop signal ends the wait, so an orchestrator waiting on a stuck migration still shuts down.
func TestWaitForSchema_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := waitForSchema(ctx, func() ([]string, error) { return []string{"work_items"}, nil }, 5*time.Millisecond, func(string, ...any) {})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
