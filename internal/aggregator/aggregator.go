package aggregator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"helius/internal/config"
)

// Run backfills all missing daily summaries up to yesterday (local time) for
// every configured aggregation, then recomputes monthly totals.
func Run(ctx context.Context, pool *pgxpool.Pool, cfgs []config.AggregationConfig) error {
	if len(cfgs) == 0 {
		log.Println("aggregate: no aggregations configured, nothing to do")
		return nil
	}

	now := time.Now()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())

	for _, cfg := range cfgs {
		if err := runOne(ctx, pool, cfg, yesterday); err != nil {
			return fmt.Errorf("aggregation %q: %w", cfg.Name, err)
		}
	}
	return nil
}

// groupKey identifies one distinct time-series within an aggregation config.
type groupKey struct {
	// name is the value written to aggregation_summary.name.
	// When a channel column is configured it has the form "configName_ch{N}".
	name     string
	sensorID string
	// channelVal is non-nil only when cfg.ChannelColumn is set.
	channelVal *int
}

func runOne(ctx context.Context, pool *pgxpool.Pool, cfg config.AggregationConfig, yesterday time.Time) error {
	tsCol := cfg.TimestampColumn
	if tsCol == "" {
		tsCol = "recorded_at"
	}

	keys, err := listGroupKeys(ctx, pool, cfg)
	if err != nil {
		return fmt.Errorf("list group keys: %w", err)
	}
	if len(keys) == 0 {
		log.Printf("aggregate %q: no source rows found, skipping", cfg.Name)
		return nil
	}

	for _, key := range keys {
		missing, err := missingDays(ctx, pool, cfg, tsCol, key, yesterday)
		if err != nil {
			return fmt.Errorf("missing days for %q/%s: %w", key.name, key.sensorID, err)
		}
		for _, day := range missing {
			val, err := computeDay(ctx, pool, cfg, tsCol, key, day)
			if err != nil {
				return fmt.Errorf("compute day %s %q/%s: %w", day.Format("2006-01-02"), key.name, key.sensorID, err)
			}
			if err := upsertSummary(ctx, pool, key, "day", day, val); err != nil {
				return fmt.Errorf("upsert day %s %q/%s: %w", day.Format("2006-01-02"), key.name, key.sensorID, err)
			}
			log.Printf("aggregate: %q/%s %s → %.3f", key.name, key.sensorID, day.Format("2006-01-02"), val)
		}
	}

	if err := recomputeMonthly(ctx, pool, keys, yesterday); err != nil {
		return fmt.Errorf("recompute monthly: %w", err)
	}
	return nil
}

// listGroupKeys returns all distinct (sensor_id[, channel]) groups that have
// at least one non-null source_column reading.
func listGroupKeys(ctx context.Context, pool *pgxpool.Pool, cfg config.AggregationConfig) ([]groupKey, error) {
	tableQ := pgx.Identifier{cfg.SourceTable}.Sanitize()
	colQ := pgx.Identifier{cfg.SourceColumn}.Sanitize()

	var (
		rows pgx.Rows
		err  error
	)
	if cfg.ChannelColumn != "" {
		chanColQ := pgx.Identifier{cfg.ChannelColumn}.Sanitize()
		rows, err = pool.Query(ctx, fmt.Sprintf(`
			SELECT DISTINCT sensor_id, %s
			FROM %s
			WHERE %s IS NOT NULL
			ORDER BY sensor_id, %s
		`, chanColQ, tableQ, colQ, chanColQ))
	} else {
		rows, err = pool.Query(ctx, fmt.Sprintf(`
			SELECT DISTINCT sensor_id
			FROM %s
			WHERE %s IS NOT NULL
			ORDER BY sensor_id
		`, tableQ, colQ))
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []groupKey
	for rows.Next() {
		if cfg.ChannelColumn != "" {
			var sensorID string
			var ch int
			if err := rows.Scan(&sensorID, &ch); err != nil {
				return nil, err
			}
			chCopy := ch
			keys = append(keys, groupKey{
				name:       fmt.Sprintf("%s_ch%d", cfg.Name, ch),
				sensorID:   sensorID,
				channelVal: &chCopy,
			})
		} else {
			var sensorID string
			if err := rows.Scan(&sensorID); err != nil {
				return nil, err
			}
			keys = append(keys, groupKey{
				name:     cfg.Name,
				sensorID: sensorID,
			})
		}
	}
	return keys, rows.Err()
}

// missingDays returns all local dates between the earliest source reading and
// yesterday (inclusive) that are not yet in aggregation_summary as 'day' rows.
func missingDays(ctx context.Context, pool *pgxpool.Pool, cfg config.AggregationConfig, tsCol string, key groupKey, yesterday time.Time) ([]time.Time, error) {
	tableQ := pgx.Identifier{cfg.SourceTable}.Sanitize()
	colQ := pgx.Identifier{cfg.SourceColumn}.Sanitize()
	tsColQ := pgx.Identifier{tsCol}.Sanitize()

	// Find the date of the earliest non-null source reading.
	var earliest time.Time
	var err error
	if key.channelVal != nil {
		chanColQ := pgx.Identifier{cfg.ChannelColumn}.Sanitize()
		err = pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT DATE_TRUNC('day', MIN(%s))
			FROM %s
			WHERE sensor_id = $1 AND %s = $2 AND %s IS NOT NULL
		`, tsColQ, tableQ, chanColQ, colQ), key.sensorID, *key.channelVal).Scan(&earliest)
	} else {
		err = pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT DATE_TRUNC('day', MIN(%s))
			FROM %s
			WHERE sensor_id = $1 AND %s IS NOT NULL
		`, tsColQ, tableQ, colQ), key.sensorID).Scan(&earliest)
	}
	if err != nil {
		return nil, err
	}

	earliestLocal := earliest.In(time.Local)
	earliestDay := time.Date(earliestLocal.Year(), earliestLocal.Month(), earliestLocal.Day(), 0, 0, 0, 0, time.Local)

	// Collect already-computed day summaries for this series.
	rows, err := pool.Query(ctx, `
		SELECT period
		FROM aggregation_summary
		WHERE name = $1 AND sensor_id = $2 AND period_type = 'day'
		  AND period >= $3 AND period <= $4
	`, key.name, key.sensorID, earliestDay, yesterday)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	done := map[string]bool{}
	for rows.Next() {
		var d time.Time
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		done[d.In(time.Local).Format("2006-01-02")] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var missing []time.Time
	for d := earliestDay; !d.After(yesterday); d = d.AddDate(0, 0, 1) {
		if !done[d.Format("2006-01-02")] {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

// computeDay calculates the aggregated value for a single calendar day using
// the method specified in cfg ("delta" or "integrate").
func computeDay(ctx context.Context, pool *pgxpool.Pool, cfg config.AggregationConfig, tsCol string, key groupKey, day time.Time) (float64, error) {
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.Local)
	dayEnd := dayStart.AddDate(0, 0, 1)

	tableQ := pgx.Identifier{cfg.SourceTable}.Sanitize()
	colQ := pgx.Identifier{cfg.SourceColumn}.Sanitize()
	tsColQ := pgx.Identifier{tsCol}.Sanitize()

	// Build the WHERE clause and args, varying by whether a channel is used.
	var whereClause string
	var args []any
	if key.channelVal != nil {
		chanColQ := pgx.Identifier{cfg.ChannelColumn}.Sanitize()
		whereClause = fmt.Sprintf(
			"sensor_id = $1 AND %s = $2 AND %s IS NOT NULL AND %s >= $3 AND %s < $4",
			chanColQ, colQ, tsColQ, tsColQ,
		)
		args = []any{key.sensorID, *key.channelVal, dayStart, dayEnd}
	} else {
		whereClause = fmt.Sprintf(
			"sensor_id = $1 AND %s IS NOT NULL AND %s >= $2 AND %s < $3",
			colQ, tsColQ, tsColQ,
		)
		args = []any{key.sensorID, dayStart, dayEnd}
	}

	var val float64
	var err error

	switch cfg.Method {
	case "delta":
		// Sum positive consecutive deltas of a monotonically increasing counter.
		// GREATEST(..., 0) ignores counter resets (e.g. device reboots).
		// The outer COALESCE handles an empty result set.
		sqlStr := fmt.Sprintf(`
			SELECT COALESCE(SUM(delta), 0)
			FROM (
				SELECT COALESCE(GREATEST(
					%s - LAG(%s) OVER (ORDER BY %s),
					0
				), 0) AS delta
				FROM %s
				WHERE %s
			) sub
		`, colQ, colQ, tsColQ, tableQ, whereClause)
		err = pool.QueryRow(ctx, sqlStr, args...).Scan(&val)

	case "integrate":
		// Left-Riemann integration: each sample is assumed to hold until the
		// next one. The last sample in the window has no LEAD → NULL, which
		// SUM ignores, meaning the final partial interval up to dayEnd is
		// dropped (small precision tradeoff, acceptable for daily summaries).
		divisor := cfg.IntegrateDivisor
		if divisor == 0 {
			divisor = 1
		}
		sqlStr := fmt.Sprintf(`
			SELECT COALESCE(SUM(%s * EXTRACT(EPOCH FROM next_ts - %s) / %g), 0)
			FROM (
				SELECT
					%s,
					%s,
					LEAD(%s) OVER (ORDER BY %s) AS next_ts
				FROM %s
				WHERE %s
			) sub
			WHERE next_ts IS NOT NULL
		`, colQ, tsColQ, divisor, colQ, tsColQ, tsColQ, tsColQ, tableQ, whereClause)
		err = pool.QueryRow(ctx, sqlStr, args...).Scan(&val)

	default:
		return 0, fmt.Errorf("unknown method %q (want \"delta\" or \"integrate\")", cfg.Method)
	}

	return val, err
}

// upsertSummary inserts or replaces a row in aggregation_summary.
func upsertSummary(ctx context.Context, pool *pgxpool.Pool, key groupKey, periodType string, period time.Time, val float64) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO aggregation_summary (name, sensor_id, period_type, period, value)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name, sensor_id, period_type, period)
		DO UPDATE SET value = EXCLUDED.value
	`, key.name, key.sensorID, periodType, period.Format("2006-01-02"), val)
	return err
}

// recomputeMonthly sums daily rows into monthly totals for every affected month.
func recomputeMonthly(ctx context.Context, pool *pgxpool.Pool, keys []groupKey, upTo time.Time) error {
	for _, key := range keys {
		rows, err := pool.Query(ctx, `
			SELECT DATE_TRUNC('month', period)::DATE AS month, SUM(value)
			FROM aggregation_summary
			WHERE name = $1 AND sensor_id = $2 AND period_type = 'day'
			  AND period <= $3
			GROUP BY month
			ORDER BY month
		`, key.name, key.sensorID, upTo)
		if err != nil {
			return err
		}

		type monthRow struct {
			month time.Time
			val   float64
		}
		var months []monthRow
		for rows.Next() {
			var mr monthRow
			if err := rows.Scan(&mr.month, &mr.val); err != nil {
				rows.Close()
				return err
			}
			months = append(months, mr)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, mr := range months {
			if err := upsertSummary(ctx, pool, key, "month", mr.month, mr.val); err != nil {
				return fmt.Errorf("upsert monthly %q/%s %s: %w", key.name, key.sensorID, mr.month.Format("2006-01"), err)
			}
			log.Printf("aggregate: %q/%s %s → %.3f (monthly)", key.name, key.sensorID, mr.month.Format("2006-01"), mr.val)
		}
	}
	return nil
}
