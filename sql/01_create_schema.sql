-- =============================================================================
-- 01_create_schema.sql
-- Aviation & Weather Intelligence Matrix
-- Creates all tables for flight telemetry and weather observation storage
-- =============================================================================

-- Drop tables if they exist (safe re-run)
DROP TABLE IF EXISTS flight_weather_correlation CASCADE;
DROP TABLE IF EXISTS weather_observations CASCADE;
DROP TABLE IF EXISTS flight_telemetry CASCADE;
DROP TABLE IF EXISTS airports CASCADE;

-- -----------------------------------------------------------------------------
-- AIRPORTS — reference table for all monitored airports
-- -----------------------------------------------------------------------------
CREATE TABLE airports (
    airport_code        VARCHAR(10)     PRIMARY KEY,
    airport_name        VARCHAR(100)    NOT NULL,
    latitude            DECIMAL(9, 6)   NOT NULL,
    longitude           DECIMAL(9, 6)   NOT NULL,
    city                VARCHAR(50),
    country             VARCHAR(50)
);

-- -----------------------------------------------------------------------------
-- FLIGHT_TELEMETRY — one row per aircraft per ingestion cycle
-- -----------------------------------------------------------------------------
CREATE TABLE flight_telemetry (
    id                  SERIAL          PRIMARY KEY,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    icao24              VARCHAR(10)     NOT NULL,
    callsign            VARCHAR(20),
    origin_country      VARCHAR(50),
    timestamp_utc       TIMESTAMPTZ,
    last_contact_utc    TIMESTAMPTZ,
    latitude            DECIMAL(9, 6),
    longitude           DECIMAL(9, 6),
    altitude_m          DECIMAL(10, 2),
    geo_altitude_m      DECIMAL(10, 2),
    velocity_ms         DECIMAL(8, 2),
    heading_deg         DECIMAL(6, 2),
    vertical_rate_ms    DECIMAL(8, 2),
    squawk              VARCHAR(10),

    -- Derived fields added by processing.py
    altitude_category   VARCHAR(20),    -- 'low', 'cruise', 'high'
    speed_category      VARCHAR(20),    -- 'slow', 'normal', 'fast'
    nearest_airport     VARCHAR(10)     REFERENCES airports(airport_code)
);

-- -----------------------------------------------------------------------------
-- WEATHER_OBSERVATIONS — one row per airport per ingestion cycle
-- -----------------------------------------------------------------------------
CREATE TABLE weather_observations (
    id                  SERIAL          PRIMARY KEY,
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    airport_code        VARCHAR(10)     REFERENCES airports(airport_code),
    timestamp_utc       TIMESTAMPTZ,
    temp_c              DECIMAL(6, 2),
    feels_like_c        DECIMAL(6, 2),
    humidity_pct        INTEGER,
    pressure_hpa        DECIMAL(8, 2),
    wind_speed_ms       DECIMAL(8, 2),
    wind_gust_ms        DECIMAL(8, 2),
    wind_deg            INTEGER,
    visibility_m        INTEGER,
    cloud_pct           INTEGER,
    weather_main        VARCHAR(50),
    weather_desc        VARCHAR(100),
    weather_id          INTEGER,

    -- Derived severity flags added by processing.py
    has_alert           BOOLEAN         DEFAULT FALSE,
    alert_event         VARCHAR(100),
    alert_severity      VARCHAR(20),    -- 'low', 'moderate', 'severe', 'extreme'
    severity_score      INTEGER         DEFAULT 0  -- 0-100 composite score
);

-- -----------------------------------------------------------------------------
-- FLIGHT_WEATHER_CORRELATION — links flights to nearest airport's weather
-- This is the key analytical table our SQL queries will run against
-- -----------------------------------------------------------------------------
CREATE TABLE flight_weather_correlation (
    id                      SERIAL      PRIMARY KEY,
    correlated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    flight_telemetry_id     INTEGER     REFERENCES flight_telemetry(id),
    weather_observation_id  INTEGER     REFERENCES weather_observations(id),
    airport_code            VARCHAR(10) REFERENCES airports(airport_code),
    distance_to_airport_km  DECIMAL(10, 2),
    is_at_risk              BOOLEAN     DEFAULT FALSE
);

-- -----------------------------------------------------------------------------
-- INDEXES — speeds up our analytical queries significantly
-- -----------------------------------------------------------------------------
CREATE INDEX idx_flight_telemetry_icao24       ON flight_telemetry(icao24);
CREATE INDEX idx_flight_telemetry_ingested_at  ON flight_telemetry(ingested_at);
CREATE INDEX idx_flight_telemetry_callsign     ON flight_telemetry(callsign);
CREATE INDEX idx_weather_airport_code          ON weather_observations(airport_code);
CREATE INDEX idx_weather_ingested_at           ON weather_observations(ingested_at);
CREATE INDEX idx_correlation_airport           ON flight_weather_correlation(airport_code);
CREATE INDEX idx_correlation_at_risk           ON flight_weather_correlation(is_at_risk);

-- -----------------------------------------------------------------------------
-- SEED AIRPORTS — insert our 12 monitored airports
-- -----------------------------------------------------------------------------
INSERT INTO airports (airport_code, airport_name, latitude, longitude, city, country) VALUES
    ('DEL', 'Indira Gandhi International',  28.5562,  77.1000, 'Delhi',     'India'),
    ('BOM', 'Chhatrapati Shivaji Maharaj',  19.0896,  72.8656, 'Mumbai',    'India'),
    ('MAA', 'Chennai International',        12.9941,  80.1709, 'Chennai',   'India'),
    ('HYD', 'Rajiv Gandhi International',   17.2403,  78.4294, 'Hyderabad', 'India'),
    ('BLR', 'Kempegowda International',     13.1986,  77.7066, 'Bengaluru', 'India'),
    ('CCU', 'Netaji Subhas Chandra Bose',   22.6520,  88.4463, 'Kolkata',   'India'),
    ('AMD', 'Sardar Vallabhbhai Patel',     23.8433,  72.6347, 'Ahmedabad', 'India'),
    ('JAI', 'Jaipur International',         26.8242,  75.8122, 'Jaipur',    'India'),
    ('KHI', 'Jinnah International',         24.6962,  67.1609, 'Karachi',   'Pakistan'),
    ('DAC', 'Hazrat Shahjalal International',23.7647, 90.3784, 'Dhaka',     'Bangladesh'),
    ('CMB', 'Bandaranaike International',    7.1800,  79.8847, 'Colombo',   'Sri Lanka'),
    ('KTM', 'Tribhuvan International',      27.6966,  85.3591, 'Kathmandu', 'Nepal');