package aggregator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Run backfills all missing daily summaries up to yesterday (local time) for
// every (sensor_id, channel) pair that has energy_wh readings, then recomputes
// the monthly totals for any affected months.
func Run(ctx context.Context, pool *pgxpool.Pool) error {
	now := time.Now()
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())

	pairs, err := sensorChannelPairs(ctx, pool)
	if err != nil {
		return fmt.Errorf("list sensor/channel pairs: %w", err)
	}
	if len(pairs) == 0 {
		log.Println("aggregate: no energy_wh readings found, nothing to do")
		return nil
	}

	for _, p := range pairs {
		missing, err := missingDays(ctx, pool, p.sensorID, p.channel, yesterday)
		if err != nil {
			return fmt.Errorf("missing days for %s ch%d: %w", p.sensorID, p.channel, err)
		}
		for _, day := range missing {
			wh, err := computeDay(ctx, pool, p.sensorID, p.channel, day)
			if err != nil {
				return fmt.Errorf("compute day %s %s ch%d: %w", day, p.sensorID, p.channel, err)
			}
			if err := upsertSummary(ctx, pool, p.sensorID, p.channel, "day", day, wh); err != nil {
				return fmt.Errorf("upsert daily %s %s ch%d: %w", day, p.sensorID, p.channel, err)
			}
			log.Printf("aggregate: %s ch%d %s → %.1f Wh", p.sensorID, p.channel, day.Format("2006-01-02"), wh)
		}
	}

	// Recompute monthly totals.
	if err := recomputeMonthlyFromDaily(ctx, pool, pairs, yesterday); err != nil {
		return fmt.Errorf("recompute monthly: %w", err)
	}

	return nil
}

type sensorChannel struct {
	sensorID string
	channel  int
}

// sensorChannelPairs returns all distinct (sensor_id, channel) pairs that have
// at least one non-null energy_wh reading.
func sensorChannelPairs(ctx context.Context, pool *pgxpool.Pool) ([]sensorChannel, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT sensor_id, channel
		FROM energy_readings
		WHERE energy_wh IS NOT NULL
		ORDER BY sensor_id, channel
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []sensorChannel
	for rows.Next() {
		var p sensorChannel
		if err := rows.Scan(&p.sensorID, &p.channel); err != nil {
			return nil, err
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

// missingDays returns all local dates between the first reading date and
// yesterday (inclusive) that are not yet in energy_summary as 'day' rows.
func missingDays(ctx context.Context, pool *pgxpool.Pool, sensorID string, channel int, yesterday time.Time) ([]time.Time, error) {
	var earliest time.Time
	err := pool.QueryRow(ctx, `
		SELECT DATE_TRUNC('day', MIN(recorded_at))
		FROM energy_readings
		WHERE sensor_id = $1 AND channel = $2 AND energy_wh IS NOT NULL
	`, sensorID, channel).Scan(&earliest)
	if err != nil {
		return nil, err
	}
	earliestLocal := earliest.In(time.Local)
	earliestDay := time.Date(earliestLocal.Year(), earliestLocal.Month(), earliestLocal.Day(), 0, 0, 0, 0, time.Local)

	// Collect all already-computed days.
	rows, err := pool.Query(ctx, `
		SELECT period FROM energy_summary
		WHERE sensor_id = $1 AND channel = $2 AND period_type = 'day'
		  AND period >= $3 AND period <= $4
	`, sensorID, channel, earliestDay, yesterday)
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

// computeDay sums positive consecutive deltas of energy_wh for the given local
// day, handling counter resets (e.g. device reboot) by ignoring negative jumps.
func computeDay(ctx context.Context, pool *pgxpool.Pool, sensorID string, channel int, day time.Time) (float64, error) {
	dayStart := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.Local)
	dayEnd := dayStart.AddDate(0, 0, 1)

	var wh float64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(delta), 0)
		FROM (
			SELECT COALESCE(GREATEST(
				energy_wh - LAG(energy_wh) OVER (ORDER BY recorded_at),
				0
			), 0) AS delta
			FROM energy_readings
			WHERE sensor_id = $1
			  AND channel   = $2
			  AND energy_wh IS NOT NULL
			  AND recorded_at >= $3
			  AND recorded_at <  $4
		) sub
	`, sensorID, channel, dayStart, dayEnd).Scan(&wh)
	return wh, err
}

// upsertSummary inserts or updates a row in energy_summary.
func upsertSummary(ctx context.Context, pool *pgxpool.Pool, sensorID string, channel int, periodType string, period time.Time, wh float64) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO energy_summary (sensor_id, channel, period_type, period, energy_wh)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (sensor_id, channel, period_type, period)
		DO UPDATE SET energy_wh = EXCLUDED.energy_wh
	`, sensorID, channel, periodType, period.Format("2006-01-02"), wh)
	return err
}

// recomputeMonthlyFromDaily recalculates monthly totals from daily summaries
// for all (sensor_id, channel) pairs, for every month up to the current one.
func recomputeMonthlyFromDaily(ctx context.Context, pool *pgxpool.Pool, pairs []sensorChannel, upTo time.Time) error {
	for _, p := range pairs {
		rows, err := pool.Query(ctx, `
			SELECT DATE_TRUNC('month', period)::DATE AS month, SUM(energy_wh)
			FROM energy_summary
			WHERE sensor_id = $1 AND channel = $2 AND period_type = 'day'
			  AND period <= $3
			GROUP BY month
			ORDER BY month
		`, p.sensorID, p.channel, upTo)
		if err != nil {
			return err
		}

		type monthRow struct {
			month time.Time
			wh    float64
		}
		var months []monthRow
		for rows.Next() {
			var mr monthRow
			if err := rows.Scan(&mr.month, &mr.wh); err != nil {
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
			if err := upsertSummary(ctx, pool, p.sensorID, p.channel, "month", mr.month, mr.wh); err != nil {
				return fmt.Errorf("upsert monthly %s ch%d %s: %w", p.sensorID, p.channel, mr.month.Format("2006-01"), err)
			}
			log.Printf("aggregate: %s ch%d %s → %.1f Wh (monthly)", p.sensorID, p.channel, mr.month.Format("2006-01"), mr.wh)
		}
	}
	return nil
}
