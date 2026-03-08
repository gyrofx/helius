package db

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dsn)
}

// ConnectWithSchema returns a pool whose connections always operate inside the
// given Postgres schema (via search_path). Used by the test helper to achieve
// per-test schema isolation without Docker.
func ConnectWithSchema(ctx context.Context, dsn, schemaName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName
	return pgxpool.NewWithConfig(ctx, cfg)
}

func ApplySchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}
