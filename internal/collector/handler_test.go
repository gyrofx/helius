package collector_test

import (
	"context"
	"encoding/json"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"helius/internal/collector"
	"helius/internal/config"
	"helius/internal/testhelper"
)

// fakeMQTTMsg implements mqtt.Message with just a payload.
type fakeMQTTMsg struct {
	topic   string
	payload []byte
}

func (m *fakeMQTTMsg) Duplicate() bool   { return false }
func (m *fakeMQTTMsg) Qos() byte         { return 0 }
func (m *fakeMQTTMsg) Retained() bool    { return false }
func (m *fakeMQTTMsg) Topic() string     { return m.topic }
func (m *fakeMQTTMsg) MessageID() uint16 { return 0 }
func (m *fakeMQTTMsg) Payload() []byte   { return m.payload }
func (m *fakeMQTTMsg) Ack()              {}

// compile-time interface assertion
var _ mqtt.Message = (*fakeMQTTMsg)(nil)


// TestMakeHandler_JSONPayload verifies that a JSON payload is parsed and
// inserted into the correct table/columns.
func TestMakeHandler_JSONPayload(t *testing.T) {
	pool := testhelper.SetupDB(t, "sensor1")

	hcfg := config.HandlerConfig{
		Topic:    "home/temp",
		Table:    "temperature_readings",
		SensorID: "sensor1",
		Fields: map[string]string{
			"temperature": "temperature_c",
			"humidity":    "humidity_pct",
		},
	}
	handler := collector.MakeHandler(pool, hcfg)

	payload, _ := json.Marshal(map[string]any{
"temperature": 21.5,
"humidity":    55.0,
})
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: payload})

	var tempC, humPct float64
	err := pool.QueryRow(context.Background(),
		"SELECT temperature_c, humidity_pct FROM temperature_readings WHERE sensor_id = $1",
		"sensor1",
	).Scan(&tempC, &humPct)
	if err != nil {
		t.Fatalf("query inserted row: %v", err)
	}
	if tempC != 21.5 {
		t.Errorf("temperature_c = %v, want 21.5", tempC)
	}
	if humPct != 55.0 {
		t.Errorf("humidity_pct = %v, want 55.0", humPct)
	}
}

// TestMakeHandler_RawFloatPayload verifies that a plain numeric payload is
// inserted into the column specified by ValueColumn.
func TestMakeHandler_RawFloatPayload(t *testing.T) {
	pool := testhelper.SetupDB(t, "em1")

	ch := 0
	hcfg := config.HandlerConfig{
		Topic:       "home/energy/power",
		Table:       "energy_readings",
		SensorID:    "em1",
		Channel:     &ch,
		ValueColumn: "power_w",
	}
	handler := collector.MakeHandler(pool, hcfg)
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: []byte("1234.5\n")})

	var powerW float64
	err := pool.QueryRow(context.Background(),
		"SELECT power_w FROM energy_readings WHERE sensor_id = $1 AND channel = $2",
		"em1", 0,
	).Scan(&powerW)
	if err != nil {
		t.Fatalf("query inserted row: %v", err)
	}
	if powerW != 1234.5 {
		t.Errorf("power_w = %v, want 1234.5", powerW)
	}
}

// TestMakeHandler_InvalidFloat verifies that a non-numeric payload does not
// produce an insert when ValueColumn is set.
func TestMakeHandler_InvalidFloat(t *testing.T) {
	pool := testhelper.SetupDB(t, "em2")

	ch := 0
	hcfg := config.HandlerConfig{
		Topic:       "home/energy/bad",
		Table:       "energy_readings",
		SensorID:    "em2",
		Channel:     &ch,
		ValueColumn: "power_w",
	}
	handler := collector.MakeHandler(pool, hcfg)
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: []byte("not-a-number")})

	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM energy_readings WHERE sensor_id = $1", "em2",
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows for invalid float payload, got %d", count)
	}
}

// TestMakeHandler_InvalidJSON verifies that malformed JSON does not produce
// an insert when Fields are configured.
func TestMakeHandler_InvalidJSON(t *testing.T) {
	pool := testhelper.SetupDB(t, "sensor_bad")

	hcfg := config.HandlerConfig{
		Topic:    "home/temp/bad",
		Table:    "temperature_readings",
		SensorID: "sensor_bad",
		Fields:   map[string]string{"temperature": "temperature_c"},
	}
	handler := collector.MakeHandler(pool, hcfg)
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: []byte("{bad json")})

	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM temperature_readings WHERE sensor_id = $1", "sensor_bad",
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows for invalid JSON payload, got %d", count)
	}
}

// TestMakeHandler_Throttle verifies that a second message within the throttle
// window is dropped and only one row is inserted.
func TestMakeHandler_Throttle(t *testing.T) {
	pool := testhelper.SetupDB(t, "sensor_t")

	hcfg := config.HandlerConfig{
		Topic:    "home/temp/throttled",
		Table:    "temperature_readings",
		SensorID: "sensor_t",
		Fields:   map[string]string{"temperature": "temperature_c"},
		Throttle: "1m", // 1-minute window → second call within same second is dropped
	}
	handler := collector.MakeHandler(pool, hcfg)

	mkPayload := func(temp float64) []byte {
		b, _ := json.Marshal(map[string]any{"temperature": temp})
		return b
	}

	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: mkPayload(10.0)})
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: mkPayload(20.0)}) // throttled

	var count int
	if err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM temperature_readings WHERE sensor_id = $1", "sensor_t",
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after throttle, got %d", count)
	}
}

// TestMakeHandler_Channel verifies that the channel value is persisted when
// HandlerConfig.Channel is set.
func TestMakeHandler_Channel(t *testing.T) {
	pool := testhelper.SetupDB(t, "em3")

	ch := 2
	hcfg := config.HandlerConfig{
		Topic:       "shellies/ch2/power",
		Table:       "energy_readings",
		SensorID:    "em3",
		Channel:     &ch,
		ValueColumn: "power_w",
	}
	handler := collector.MakeHandler(pool, hcfg)
	handler(nil, &fakeMQTTMsg{topic: hcfg.Topic, payload: []byte("500.0")})

	var gotChannel int
	if err := pool.QueryRow(context.Background(),
		"SELECT channel FROM energy_readings WHERE sensor_id = $1", "em3",
	).Scan(&gotChannel); err != nil {
		t.Fatalf("query channel: %v", err)
	}
	if gotChannel != 2 {
		t.Errorf("channel = %d, want 2", gotChannel)
	}
}
