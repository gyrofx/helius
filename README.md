# helius

Home energy and environment monitoring collector. Subscribes to MQTT topics from Shelly, ESPHome and other sensors, stores readings in PostgreSQL, and aggregates daily/monthly energy summaries for Grafana dashboards.

## Architecture

```
MQTT broker
    │
    ▼
helius serve          ← long-running collector
    │  subscribes to configured topics
    │  inserts rows into PostgreSQL
    ▼
PostgreSQL
    │
    ▼
helius aggregate      ← short-lived batch job (run daily via cron)
    │  backfills missing days
    │  upserts energy_summary (day + month)
    ▼
Grafana
```

## Commands

| Command | Description |
|---|---|
| `helius serve` | Start the MQTT collector (runs until interrupted) |
| `helius aggregate` | Compute daily/monthly energy summaries and exit |

## Configuration

All configuration is in `config.yaml`:

```yaml
mqtt:
  broker: "tcp://host:1883"
  client_id: "helius"

database:
  dsn: "postgres://user:pass@host:5432/helius?sslmode=disable"

sensors:
  - id: "my_sensor"
    name: "My Sensor"
    type: "energy_meter"   # energy_meter | temperature_sensor | heat_pump

handlers:
  # JSON payload — map JSON keys to DB columns
  - topic: "shellyplusht-XXXX/status/temperature:0"
    table: "temperature_readings"
    sensor_id: "my_sensor"
    fields:
      tC: "temperature_c"

  # Raw float payload
  - topic: "rehalp/sensor/temperature/state"
    table: "temperature_readings"
    sensor_id: "my_sensor"
    value_column: "temperature_c"
    throttle: "30s"   # optional: minimum interval between inserts
```

### Handler types

**JSON payload** (`fields` map): parses the MQTT payload as JSON and maps specified keys to database columns. Used for Gen2 Shellies (`shellyplusht-*`, `shellypmmini-*`).

**Raw float** (`value_column`): parses the payload as a plain float and stores it in the named column. Used for ESPHome sensors and Gen1 Shellies (`shellies/*`).

### Throttling

Set `throttle: "30s"` (any Go duration) on any handler to drop messages arriving faster than the interval. Useful for Gen1 Shellies that publish every 2–10 seconds.

## Database schema

| Table | Contents |
|---|---|
| `sensors` | Sensor registry |
| `temperature_readings` | Temperature (°C) and humidity (%) |
| `energy_readings` | Power (W), energy (Wh), voltage (V), current (A) — sparse rows for Gen1 devices |
| `heatpump_readings` | Ovum heat pump telemetry |
| `gas_readings` | Gas consumption (m³) |
| `solar_readings` | Solar power, energy, battery |
| `weather_readings` | Outdoor weather |
| `energy_summary` | Pre-aggregated daily and monthly energy totals |

### energy_summary

Populated by `helius aggregate`. Period types: `'day'`, `'month'`.

```sql
SELECT period, energy_wh
FROM energy_summary
WHERE sensor_id = 'shelly_em3_main'
  AND channel     = 0
  AND period_type = 'day'
ORDER BY period;
```

## Running with Docker Compose

```bash
# Start the collector
docker compose up -d helius

# Run the aggregator once
docker compose run --rm aggregate
```

### Daily cron

```
5 0 * * * docker compose -f /path/to/helius/docker-compose.yml run --rm aggregate
```

Runs at 00:05 local time. On first run it backfills all historical days automatically.

## Running locally

```bash
go build -o helius .

./helius serve
./helius aggregate
```

## Supported devices

| Device | Generation | Protocol |
|---|---|---|
| Shelly Plus HT | Gen2 | JSON status topics |
| Shelly PM Mini | Gen2 | JSON status topics |
| Shelly EM3 | Gen1 | Per-metric float topics |
| Shelly EM | Gen1 | Per-metric float topics |
| ESPHome sensors | — | Plain float state topics |
| Ovum heat pump | — | JSON status topic |
