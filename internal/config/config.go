package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MQTT         MQTTConfig          `yaml:"mqtt"`
	Database     DatabaseConfig      `yaml:"database"`
	Sensors      []SensorDef         `yaml:"sensors"`
	Handlers     []HandlerConfig     `yaml:"handlers"`
	Aggregations []AggregationConfig `yaml:"aggregations"`
}

type MQTTConfig struct {
	Broker   string `yaml:"broker"`
	ClientID string `yaml:"client_id"`
}

type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

type SensorDef struct {
	ID   string `yaml:"id"`
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

type HandlerConfig struct {
	Topic       string            `yaml:"topic"`
	Table       string            `yaml:"table"`
	SensorID    string            `yaml:"sensor_id"`
	Channel     *int              `yaml:"channel,omitempty"`
	Fields      map[string]string `yaml:"fields,omitempty"`
	ValueColumn string            `yaml:"value_column,omitempty"` // payload is a raw float → insert into this column
	Throttle    string            `yaml:"throttle,omitempty"`     // min interval between inserts, e.g. "30s"
}

// AggregationConfig describes one named time-series aggregation written to
// aggregation_summary by `helius aggregate`.
//
// method: "delta"     — sums positive consecutive deltas of a monotonically
//
//	increasing counter (e.g. energy_wh odometer).
//
// method: "integrate" — left-Riemann integration of an instantaneous value
//
//	over time (e.g. power_w → Wh when divisor = 3600).
type AggregationConfig struct {
	// Name is the series key stored in aggregation_summary.name.
	// When channel_column is set, the channel value is appended: "name_ch0".
	Name string `yaml:"name"`

	// SourceTable and SourceColumn identify the raw readings to aggregate.
	SourceTable  string `yaml:"source_table"`
	SourceColumn string `yaml:"source_column"`

	// ChannelColumn is optional. When set the aggregator groups by
	// (sensor_id, channel_column) and appends "_ch{N}" to Name.
	ChannelColumn string `yaml:"channel_column,omitempty"`

	// TimestampColumn defaults to "recorded_at".
	TimestampColumn string `yaml:"timestamp_column,omitempty"`

	// Method is "delta" or "integrate".
	Method string `yaml:"method"`

	// IntegrateDivisor scales the raw integral. Use 3600 to convert W·s → Wh.
	// Defaults to 1 if omitted.
	IntegrateDivisor float64 `yaml:"integrate_divisor,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
