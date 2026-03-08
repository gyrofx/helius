// Package testhelper provides utilities for integration tests that require a
// real PostgreSQL database.
//
// Tests that call SetupDB are skipped automatically when TEST_DATABASE_URL is
// not set, so the standard `go test ./...` (no env var) only runs unit tests.
// Integration tests are opt-in by providing the env var, e.g.:
//
//	TEST_DATABASE_URL=postgres://helius:helius@localhost:5436/helius?sslmode=disable \
//	  go test -timeout 2m ./...
package testhelper

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"helius/internal/db"
)

// schemaInitLockID is the Postgres advisory-lock key used to serialise
// ApplySchema calls across all test-binary processes. The schema contains
// global DDL (CREATE ROLE, GRANT CONNECT ON DATABASE) that updates shared
// system-catalog rows; running it concurrently produces "tuple concurrently
// updated".
const schemaInitLockID = 0x68656c69 // "heli" in hex

// SetupDB connects to the database given by TEST_DATABASE_URL, creates a
// unique Postgres schema for this test (e.g. "test_3f8a…"), applies the full
// application schema inside it, and seeds the sensors table with any provided
// sensor IDs.
//
// Cleanup (DROP SCHEMA … CASCADE + pool.Close) is registered via t.Cleanup so
// callers do not need a teardown function.
//
// The test is skipped when TEST_DATABASE_URL is not set.
func SetupDB(t *testing.T, sensorIDs ...string) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()

	// Unique schema name for this test so parallel tests never collide.
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatalf("generate schema name: %v", err)
	}
	schemaName := "test_" + hex.EncodeToString(buf[:])

	// adminPool is kept open across schema creation + DDL application so that
	// the session-level advisory lock below stays held for the full window.
	adminPool, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}

	// Acquire a session-level advisory lock to serialise the global DDL in
	// schema.sql (CREATE ROLE, GRANT CONNECT ON DATABASE …) across all test-
	// binary processes. Without this, concurrent test packages trigger
	// "tuple concurrently updated" on shared system-catalog rows.
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_lock(%d)", schemaInitLockID)); err != nil {
		adminPool.Close()
		t.Fatalf("acquire advisory lock: %v", err)
	}

	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %q", schemaName)); err != nil {
		_, _ = adminPool.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_unlock(%d)", schemaInitLockID))
		adminPool.Close()
		t.Fatalf("create test schema %q: %v", schemaName, err)
	}

	// Pool whose connections always land in the test schema.
	pool, err := db.ConnectWithSchema(ctx, dsn, schemaName)
	if err != nil {
		_, _ = adminPool.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_unlock(%d)", schemaInitLockID))
		adminPool.Close()
		dropSchema(dsn, schemaName)
		t.Fatalf("connect with schema: %v", err)
	}

	err = db.ApplySchema(ctx, pool)
	_, _ = adminPool.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_unlock(%d)", schemaInitLockID))
	adminPool.Close()
	if err != nil {
		pool.Close()
		dropSchema(dsn, schemaName)
		t.Fatalf("apply schema: %v", err)
	}

	for _, id := range sensorIDs {
		if _, err := pool.Exec(ctx,
			"INSERT INTO sensors(id, name, type) VALUES ($1, $2, $3)",
			id, id+" (test)", "test",
		); err != nil {
			pool.Close()
			dropSchema(dsn, schemaName)
			t.Fatalf("seed sensor %q: %v", id, err)
		}
	}

	t.Cleanup(func() {
		pool.Close()
		dropSchema(dsn, schemaName)
	})

	return pool
}

func dropSchema(dsn, schemaName string) {
	p, err := db.Connect(context.Background(), dsn)
	if err != nil {
		return
	}
	defer p.Close()
	_, _ = p.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA %q CASCADE", schemaName))
}
