package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MQTT     MQTTConfig     `yaml:"mqtt"`
	Database DatabaseConfig `yaml:"database"`
	Sensors  []SensorDef    `yaml:"sensors"`
	Handlers []HandlerConfig `yaml:"handlers"`
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
