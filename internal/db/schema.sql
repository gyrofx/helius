CREATE TABLE IF NOT EXISTS sensors (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS temperature_readings (
    id            BIGSERIAL PRIMARY KEY,
    sensor_id     TEXT NOT NULL REFERENCES sensors(id),
    temperature_c DOUBLE PRECISION,   -- nullable: Gen2 devices publish temp/humidity on separate topics
    humidity_pct  DOUBLE PRECISION,
    recorded_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS energy_readings (
    id          BIGSERIAL PRIMARY KEY,
    sensor_id   TEXT NOT NULL REFERENCES sensors(id),
    channel     SMALLINT NOT NULL DEFAULT 0,
    power_w     DOUBLE PRECISION,
    energy_wh   DOUBLE PRECISION,
    voltage_v   DOUBLE PRECISION,
    current_a   DOUBLE PRECISION,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS heatpump_readings (
    id                            BIGSERIAL PRIMARY KEY,
    sensor_id                     TEXT NOT NULL REFERENCES sensors(id),
    inverter_rps                  DOUBLE PRECISION,
    inverter_power                DOUBLE PRECISION,
    drive_temperature             DOUBLE PRECISION,
    running_time                  DOUBLE PRECISION,
    working_hours                 DOUBLE PRECISION,
    heating_power                 DOUBLE PRECISION,
    weekly_heating_energy         DOUBLE PRECISION,
    monthly_heating_energy        DOUBLE PRECISION,
    yearly_heating_energy         DOUBLE PRECISION,
    total_heating_energy          DOUBLE PRECISION,
    cop_weekly                    DOUBLE PRECISION,
    cop_monthly                   DOUBLE PRECISION,
    cop_yearly                    DOUBLE PRECISION,
    cop_total                     DOUBLE PRECISION,
    ambient_temp_avg              DOUBLE PRECISION,
    outdoor_temp                  DOUBLE PRECISION,
    controller_temp               DOUBLE PRECISION,
    dhw_tank_upper_temp           DOUBLE PRECISION,
    dhw_tank_middle_temp          DOUBLE PRECISION,
    dhw_tank_lower_temp           DOUBLE PRECISION,
    ground_source_in              DOUBLE PRECISION,
    ground_source_out             DOUBLE PRECISION,
    ground_source_pump            DOUBLE PRECISION,
    heat_circle_1_flow_temp       DOUBLE PRECISION,
    heat_circle_1_return_temp     DOUBLE PRECISION,
    heat_circle_1_return_set_temp DOUBLE PRECISION,
    heat_circle_1_pump            DOUBLE PRECISION,
    tap_water_temp                DOUBLE PRECISION,
    tap_act_temp                  DOUBLE PRECISION,
    tap_pump_min                  DOUBLE PRECISION,
    tap_pump_percent              DOUBLE PRECISION,
    recorded_at                   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gas_readings (
    id                   BIGSERIAL PRIMARY KEY,
    sensor_id            TEXT NOT NULL REFERENCES sensors(id),
    consumption_total_m3 DOUBLE PRECISION,
    recorded_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS solar_readings (
    id                BIGSERIAL PRIMARY KEY,
    sensor_id         TEXT NOT NULL REFERENCES sensors(id),
    power_w           DOUBLE PRECISION,
    energy_today_wh   DOUBLE PRECISION,
    energy_total_wh   DOUBLE PRECISION,
    battery_soc_pct   DOUBLE PRECISION,
    battery_voltage_v DOUBLE PRECISION,
    recorded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS weather_readings (
    id            BIGSERIAL PRIMARY KEY,
    sensor_id     TEXT NOT NULL REFERENCES sensors(id),
    temp_c        DOUBLE PRECISION,
    humidity_pct  DOUBLE PRECISION,
    pressure_hpa  DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    clouds_pct    DOUBLE PRECISION,
    recorded_at   TIMESTAMPTZ DEFAULT NOW()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana') THEN
    CREATE ROLE grafana LOGIN PASSWORD 'grafana';
  END IF;
END$$;

GRANT CONNECT ON DATABASE helius TO grafana;
GRANT USAGE ON SCHEMA public TO grafana;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana;
