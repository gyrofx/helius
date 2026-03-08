package config_test

import (
	"os"
	"testing"

	"helius/internal/config"
)

func TestLoad_ValidFile(t *testing.T) {
	cfg, err := config.Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	// MQTT
	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("MQTT.Broker = %q, want %q", cfg.MQTT.Broker, "tcp://localhost:1883")
	}
	if cfg.MQTT.ClientID != "test-client" {
		t.Errorf("MQTT.ClientID = %q, want %q", cfg.MQTT.ClientID, "test-client")
	}

	// Database
	if cfg.Database.DSN == "" {
		t.Error("Database.DSN is empty")
	}

	// Sensors
	if len(cfg.Sensors) != 2 {
		t.Fatalf("len(Sensors) = %d, want 2", len(cfg.Sensors))
	}
	if cfg.Sensors[0].ID != "sensor1" {
		t.Errorf("Sensors[0].ID = %q, want %q", cfg.Sensors[0].ID, "sensor1")
	}
	if cfg.Sensors[0].Type != "temperature_sensor" {
		t.Errorf("Sensors[0].Type = %q, want %q", cfg.Sensors[0].Type, "temperature_sensor")
	}

	// Handlers
	if len(cfg.Handlers) != 2 {
		t.Fatalf("len(Handlers) = %d, want 2", len(cfg.Handlers))
	}
	h0 := cfg.Handlers[0]
	if h0.Topic != "home/temp/sensor1" {
		t.Errorf("Handlers[0].Topic = %q, want %q", h0.Topic, "home/temp/sensor1")
	}
	if h0.Table != "temperature_readings" {
		t.Errorf("Handlers[0].Table = %q, want %q", h0.Table, "temperature_readings")
	}
	if h0.Fields["temperature"] != "temperature_c" {
		t.Errorf("Handlers[0].Fields[temperature] = %q, want %q", h0.Fields["temperature"], "temperature_c")
	}

	h1 := cfg.Handlers[1]
	if h1.ValueColumn != "power_w" {
		t.Errorf("Handlers[1].ValueColumn = %q, want %q", h1.ValueColumn, "power_w")
	}
	if h1.Throttle != "30s" {
		t.Errorf("Handlers[1].Throttle = %q, want %q", h1.Throttle, "30s")
	}

	// Aggregations
	if len(cfg.Aggregations) != 2 {
		t.Fatalf("len(Aggregations) = %d, want 2", len(cfg.Aggregations))
	}
	a0 := cfg.Aggregations[0]
	if a0.Name != "energy_wh" {
		t.Errorf("Aggregations[0].Name = %q, want %q", a0.Name, "energy_wh")
	}
	if a0.Method != "delta" {
		t.Errorf("Aggregations[0].Method = %q, want %q", a0.Method, "delta")
	}
	if a0.ChannelColumn != "channel" {
		t.Errorf("Aggregations[0].ChannelColumn = %q, want %q", a0.ChannelColumn, "channel")
	}

	a1 := cfg.Aggregations[1]
	if a1.Method != "integrate" {
		t.Errorf("Aggregations[1].Method = %q, want %q", a1.Method, "integrate")
	}
	if a1.IntegrateDivisor != 3600 {
		t.Errorf("Aggregations[1].IntegrateDivisor = %v, want 3600", a1.IntegrateDivisor)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load("testdata/nonexistent.yaml")
	if err == nil {
		t.Fatal("Load expected error for missing file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected os.ErrNotExist, got: %v", err)
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(":\ninvalid: [yaml\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = config.Load(f.Name())
	if err == nil {
		t.Fatal("Load expected error for invalid YAML, got nil")
	}
}

func TestLoad_EmptyAggregations(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("mqtt:\n  broker: \"tcp://x:1883\"\ndatabase:\n  dsn: \"postgres://x/y\"\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if len(cfg.Aggregations) != 0 {
		t.Errorf("expected empty Aggregations, got %d", len(cfg.Aggregations))
	}
	if len(cfg.Sensors) != 0 {
		t.Errorf("expected empty Sensors, got %d", len(cfg.Sensors))
	}
}

func TestHandlerConfig_OptionalChannelIsNil(t *testing.T) {
	cfg, err := config.Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Handlers in testdata/valid.yaml have no channel field → Channel should be nil.
	for _, h := range cfg.Handlers {
		if h.Channel != nil {
			t.Errorf("handler %q: Channel = %v, want nil", h.Topic, *h.Channel)
		}
	}
}
