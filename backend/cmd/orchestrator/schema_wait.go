// Copyright (c) 2026 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"context"
	"strings"
	"time"
)

// schemaWaitInterval is how often the orchestrator checks the schema while it waits.
const schemaWaitInterval = 5 * time.Second

// waitForSchema returns once check reports nothing missing: the API, which owns the migrations,
// has migrated the database to this release. The orchestrator reads that schema directly, and on an
// upgrade it can start before the API has finished migrating (Compose starts it beside the API,
// Helm rolls its Deployment beside the API's), so its start-time sweeps of the previous release's
// work would otherwise fail on a column that is not there yet and wait a whole sweep interval for
// their next try. A failing check (the database not reachable yet) is retried the same way. It
// returns ctx's error when ctx ends first.
func waitForSchema(ctx context.Context, check func() ([]string, error), interval time.Duration, logf func(string, ...any)) error {
	for {
		missing, err := check()
		switch {
		case err != nil:
			logf("Waiting for the database schema: %v", err)
		case len(missing) == 0:
			return nil
		default:
			shown := missing
			if len(shown) > 5 {
				shown = shown[:5]
			}
			logf("Waiting for the API to migrate the database to this release: %d tables or columns missing (%s)", len(missing), strings.Join(shown, ", "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
