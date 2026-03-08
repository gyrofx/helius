package aggregator_test

import (
	"context"
	"testing"
	"time"

	"helius/internal/aggregator"
	"helius/internal/config"
	"helius/internal/testhelper"
)

// midday returns noon local time on the day that is daysBack days before today.
func midday(daysBack int) time.Time {
	now := time.Now().In(time.Local)
	return time.Date(now.Year(), now.Month(), now.Day()-daysBack, 12, 0, 0, 0, time.Local)
}

// TestRun_NoAggregations verifies that Run returns nil immediately when there
// are no aggregation configs.
func TestRun_NoAggregations(t *testing.T) {
	pool := testhelper.SetupDB(t)

	if err := aggregator.Run(context.Background(), pool, nil); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

// TestRun_DeltaMethod seeds a monotonically increasing energy counter across
// two past days and verifies that the aggregator produces correct daily deltas
// and a monthly total.
func TestRun_DeltaMethod(t *testing.T) {
	pool := testhelper.SetupDB(t, "energy1")
	ctx := context.Background()

	// Day D-3: readings 100 -> 150 -> 200  =>  delta = 100
	// Day D-2: readings 200 -> 300 -> 400  =>  delta = 200
	// Day D-1 ("yesterday") has no readings => delta = 0
	type row struct {
		ts  time.Time
		val float64
	}
	rows := []row{
		{midday(3).Add(-1 * time.Hour), 100},
		{midday(3), 150},
		{midday(3).Add(1 * time.Hour), 200},
		{midday(2).Add(-1 * time.Hour), 200},
		{midday(2), 300},
		{midday(2).Add(1 * time.Hour), 400},
	}
	for _, r := range rows {
		if _, err := pool.Exec(ctx,
			"INSERT INTO energy_readings(sensor_id, channel, energy_wh, recorded_at) VALUES ($1,$2,$3,$4)",
			"energy1", 0, r.val, r.ts,
		); err != nil {
			t.Fatalf("insert reading: %v", err)
		}
	}

	cfg := []config.AggregationConfig{{
		Name:         "energy_wh",
		SourceTable:  "energy_readings",
		SourceColumn: "energy_wh",
		Method:       "delta",
	}}
	if err := aggregator.Run(ctx, pool, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify the two non-zero day summaries.
	want := map[string]float64{
		midday(3).Format("2006-01-02"): 100,
		midday(2).Format("2006-01-02"): 200,
	}
	dayRows, err := pool.Query(ctx,
		"SELECT period, value FROM aggregation_summary WHERE name=$1 AND sensor_id=$2 AND period_type='day' AND value > 0 ORDER BY period",
		"energy_wh", "energy1",
	)
	if err != nil {
		t.Fatalf("query summaries: %v", err)
	}
	defer dayRows.Close()

	got := map[string]float64{}
	for dayRows.Next() {
		var period time.Time
		var val float64
		if err := dayRows.Scan(&period, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[period.In(time.Local).Format("2006-01-02")] = val
	}
	if err := dayRows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	for day, wantVal := range want {
		if gotVal, ok := got[day]; !ok {
			t.Errorf("missing day summary for %s", day)
		} else if gotVal != wantVal {
			t.Errorf("day %s: value = %v, want %v", day, gotVal, wantVal)
		}
	}

	// Verify monthly total: 100 + 200 = 300.
	var monthTotal float64
	if err := pool.QueryRow(ctx,
		"SELECT COALESCE(SUM(value),0) FROM aggregation_summary WHERE name=$1 AND sensor_id=$2 AND period_type='month'",
		"energy_wh", "energy1",
	).Scan(&monthTotal); err != nil {
		t.Fatalf("query monthly total: %v", err)
	}
	if monthTotal != 300 {
		t.Errorf("monthly total = %v, want 300", monthTotal)
	}
}

// TestRun_IntegrateMethod seeds instantaneous power readings and verifies
// the left-Riemann integral (Wh = W*s / 3600).
// Two readings 1 hour apart, both 1000 W => integral = 1000 * 3600 / 3600 = 1000 Wh.
func TestRun_IntegrateMethod(t *testing.T) {
	pool := testhelper.SetupDB(t, "solar1")
	ctx := context.Background()

	t0 := midday(3).Add(-30 * time.Minute)
	t1 := t0.Add(time.Hour)
	for _, r := range []struct {
		ts  time.Time
		val float64
	}{
		{t0, 1000},
		{t1, 1000},
	} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO solar_readings(sensor_id, power_w, recorded_at) VALUES ($1,$2,$3)",
			"solar1", r.val, r.ts,
		); err != nil {
			t.Fatalf("insert solar reading: %v", err)
		}
	}

	cfg := []config.AggregationConfig{{
		Name:             "solar_wh",
		SourceTable:      "solar_readings",
		SourceColumn:     "power_w",
		Method:           "integrate",
		IntegrateDivisor: 3600,
	}}
	if err := aggregator.Run(ctx, pool, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var val float64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM aggregation_summary WHERE name=$1 AND sensor_id=$2 AND period_type='day' AND period=$3",
		"solar_wh", "solar1", midday(3).Format("2006-01-02"),
	).Scan(&val); err != nil {
		t.Fatalf("query day summary: %v", err)
	}
	const wantWh = 1000.0
	if val != wantWh {
		t.Errorf("integrated Wh = %v, want %v", val, wantWh)
	}
}

// TestRun_WithChannelColumn verifies that readings are grouped by channel and
// the summary name gets the "_ch{N}" suffix.
// Channel 0: counter 0->100->300  =>  delta = 300.
// Channel 1: counter 0->50->75    =>  delta = 75.
func TestRun_WithChannelColumn(t *testing.T) {
	pool := testhelper.SetupDB(t, "em_ch")
	ctx := context.Background()

	type reading struct {
		ts      time.Time
		channel int
		val     float64
	}
	readings := []reading{
		{midday(3).Add(-1 * time.Hour), 0, 0},
		{midday(3), 0, 100},
		{midday(3).Add(1 * time.Hour), 0, 300},
		{midday(3).Add(-1 * time.Hour), 1, 0},
		{midday(3), 1, 50},
		{midday(3).Add(1 * time.Hour), 1, 75},
	}
	for _, r := range readings {
		if _, err := pool.Exec(ctx,
			"INSERT INTO energy_readings(sensor_id, channel, energy_wh, recorded_at) VALUES ($1,$2,$3,$4)",
			"em_ch", r.channel, r.val, r.ts,
		); err != nil {
			t.Fatalf("insert reading: %v", err)
		}
	}

	cfg := []config.AggregationConfig{{
		Name:          "electricity_wh",
		SourceTable:   "energy_readings",
		SourceColumn:  "energy_wh",
		ChannelColumn: "channel",
		Method:        "delta",
	}}
	if err := aggregator.Run(ctx, pool, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	day := midday(3).Format("2006-01-02")
	wantPerChannel := map[string]float64{
		"electricity_wh_ch0": 300,
		"electricity_wh_ch1": 75,
	}
	for name, wantVal := range wantPerChannel {
		var val float64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM aggregation_summary WHERE name=$1 AND sensor_id=$2 AND period_type='day' AND period=$3",
			name, "em_ch", day,
		).Scan(&val); err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		if val != wantVal {
			t.Errorf("%s day %s = %v, want %v", name, day, val, wantVal)
		}
	}
}

// TestRun_NoSourceRows ensures that Run completes without error and inserts
// nothing when there are no source readings.
func TestRun_NoSourceRows(t *testing.T) {
	pool := testhelper.SetupDB(t)

	cfg := []config.AggregationConfig{{
		Name:         "energy_wh",
		SourceTable:  "energy_readings",
		SourceColumn: "energy_wh",
		Method:       "delta",
	}}
	if err := aggregator.Run(context.Background(), pool, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM aggregation_summary",
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 summary rows, got %d", count)
	}
}

// TestRun_IdempotentRerun verifies that running the aggregator multiple times
// does not produce duplicate rows — the upsert must be idempotent.
func TestRun_IdempotentRerun(t *testing.T) {
	pool := testhelper.SetupDB(t, "energy2")
	ctx := context.Background()

	ts := midday(3)
	for i, v := range []float64{0, 100, 300} {
		if _, err := pool.Exec(ctx,
			"INSERT INTO energy_readings(sensor_id, channel, energy_wh, recorded_at) VALUES ($1,0,$2,$3)",
			"energy2", v, ts.Add(time.Duration(i)*time.Minute),
		); err != nil {
			t.Fatalf("insert reading: %v", err)
		}
	}

	cfg := []config.AggregationConfig{{
		Name:         "energy_wh",
		SourceTable:  "energy_readings",
		SourceColumn: "energy_wh",
		Method:       "delta",
	}}

	for run := 1; run <= 3; run++ {
		if err := aggregator.Run(ctx, pool, cfg); err != nil {
			t.Fatalf("Run #%d: %v", run, err)
		}
	}

	// Count distinct day rows — should be stable after any number of reruns.
	var dayCount int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM aggregation_summary WHERE name='energy_wh' AND sensor_id='energy2' AND period_type='day'",
	).Scan(&dayCount); err != nil {
		t.Fatalf("count day rows: %v", err)
	}
	if dayCount == 0 {
		t.Fatal("expected at least 1 day row after repeated runs")
	}

	// Total should be exactly dayCount day rows + 1 month row.
	var totalCount int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM aggregation_summary WHERE name='energy_wh' AND sensor_id='energy2'",
	).Scan(&totalCount); err != nil {
		t.Fatalf("count total rows: %v", err)
	}
	wantTotal := dayCount + 1
	if totalCount != wantTotal {
		t.Errorf("total rows = %d, want %d", totalCount, wantTotal)
	}
}

// TestRun_UnknownMethod verifies that an unknown aggregation method returns
// an error rather than silently producing wrong results.
func TestRun_UnknownMethod(t *testing.T) {
	pool := testhelper.SetupDB(t, "sensor_x")
	ctx := context.Background()

	// Insert one reading so listGroupKeys finds a row and computeDay is invoked.
	if _, err := pool.Exec(ctx,
		"INSERT INTO energy_readings(sensor_id, channel, energy_wh, recorded_at) VALUES ($1,0,$2,$3)",
		"sensor_x", 42, midday(3),
	); err != nil {
		t.Fatalf("insert reading: %v", err)
	}

	cfg := []config.AggregationConfig{{
		Name:         "bad_agg",
		SourceTable:  "energy_readings",
		SourceColumn: "energy_wh",
		Method:       "bogus",
	}}
	if err := aggregator.Run(ctx, pool, cfg); err == nil {
		t.Fatal("expected error for unknown method, got nil")
	}
}
