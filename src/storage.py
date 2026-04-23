"""
storage.py
----------
Handles all PostgreSQL interactions.
Inserts enriched flight and weather records and builds
the flight-weather correlation table.
"""

import logging
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection():
    """Creates and returns a PostgreSQL connection using .env credentials."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST",     "localhost"),
        port=os.getenv("DB_PORT",     "5432"),
        dbname=os.getenv("DB_NAME",   "aviation_db"),
        user=os.getenv("DB_USER",     "postgres"),
        password=os.getenv("DB_PASSWORD")
    )


def insert_weather_observations(weather_list: list[dict]) -> list[int]:
    """
    Inserts a batch of enriched weather records into weather_observations.
    Returns list of inserted row IDs for correlation mapping.
    """
    if not weather_list:
        return []

    sql = """
        INSERT INTO weather_observations (
            airport_code, timestamp_utc, temp_c, feels_like_c,
            humidity_pct, pressure_hpa, wind_speed_ms, wind_gust_ms,
            wind_deg, visibility_m, cloud_pct, weather_main,
            weather_desc, weather_id, has_alert, alert_event,
            alert_severity, severity_score
        ) VALUES (
            %(airport_code)s, %(timestamp_utc)s, %(temp_c)s, %(feels_like_c)s,
            %(humidity_pct)s, %(pressure_hpa)s, %(wind_speed_ms)s, %(wind_gust_ms)s,
            %(wind_deg)s, %(visibility_m)s, %(cloud_pct)s, %(weather_main)s,
            %(weather_desc)s, %(weather_id)s, %(has_alert)s, %(alert_event)s,
            %(alert_severity)s, %(severity_score)s
        ) RETURNING id;
    """

    # Map airport_name string → airport_code for FK reference
    NAME_TO_CODE = {
        "DEL - Delhi Indira Gandhi":          "DEL",
        "BOM - Mumbai Chhatrapati Shivaji":   "BOM",
        "MAA - Chennai":                      "MAA",
        "HYD - Hyderabad Rajiv Gandhi":       "HYD",
        "BLR - Bengaluru Kempegowda":         "BLR",
        "CCU - Kolkata Netaji Subhas":        "CCU",
        "AMD - Ahmedabad Sardar Vallabhbhai": "AMD",
        "JAI - Jaipur International":         "JAI",
        "KHI - Karachi Jinnah":              "KHI",
        "DAC - Dhaka Hazrat Shahjalal":       "DAC",
        "CMB - Colombo Bandaranaike":         "CMB",
        "KTM - Kathmandu Tribhuvan":          "KTM",
    }

    inserted_ids = []
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for w in weather_list:
                    w["airport_code"] = NAME_TO_CODE.get(w.get("airport_name"))
                    cur.execute(sql, w)
                    inserted_ids.append(cur.fetchone()[0])

        logger.info(f"Inserted {len(inserted_ids)} weather observations.")
    except Exception as e:
        logger.error(f"Weather insert failed: {e}")
        raise
    finally:
        conn.close()

    return inserted_ids


def insert_flight_telemetry(flight_list: list[dict]) -> list[int]:
    """
    Inserts a batch of enriched flight records into flight_telemetry.
    Returns list of inserted row IDs for correlation mapping.
    """
    if not flight_list:
        return []

    sql = """
        INSERT INTO flight_telemetry (
            icao24, callsign, origin_country, timestamp_utc,
            last_contact_utc, latitude, longitude, altitude_m,
            geo_altitude_m, velocity_ms, heading_deg, vertical_rate_ms,
            squawk, altitude_category, speed_category, nearest_airport
        ) VALUES (
            %(icao24)s, %(callsign)s, %(origin_country)s, %(timestamp_utc)s,
            %(last_contact_utc)s, %(latitude)s, %(longitude)s, %(altitude_m)s,
            %(geo_altitude_m)s, %(velocity_ms)s, %(heading_deg)s, %(vertical_rate_ms)s,
            %(squawk)s, %(altitude_category)s, %(speed_category)s, %(nearest_airport)s
        ) RETURNING id;
    """

    inserted_ids = []
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for f in flight_list:
                    cur.execute(sql, f)
                    inserted_ids.append(cur.fetchone()[0])

        logger.info(f"Inserted {len(inserted_ids)} flight records.")
    except Exception as e:
        logger.error(f"Flight insert failed: {e}")
        raise
    finally:
        conn.close()

    return inserted_ids


def insert_correlations(
    flight_ids: list[int],
    flight_list: list[dict],
    weather_code_to_id: dict
) -> None:
    """
    Builds the flight_weather_correlation table by linking each flight
    to the weather observation at its nearest airport.

    A flight is flagged is_at_risk when:
    - The nearest airport has an active weather alert, OR
    - The flight is within 150km of an airport with severity_score >= 35
    """
    if not flight_ids:
        return

    sql = """
        INSERT INTO flight_weather_correlation (
            flight_telemetry_id, weather_observation_id,
            airport_code, distance_to_airport_km, is_at_risk
        ) VALUES (%s, %s, %s, %s, %s);
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for fid, flight in zip(flight_ids, flight_list):
                    airport_code = flight.get("nearest_airport")
                    distance_km  = flight.get("distance_to_nearest_airport_km")
                    w_data       = weather_code_to_id.get(airport_code, {})
                    w_obs_id     = w_data.get("id")
                    severity     = w_data.get("severity_score", 0)
                    has_alert    = w_data.get("has_alert", False)

                    is_at_risk = (
                        has_alert or
                        (severity >= 35 and distance_km is not None and distance_km <= 150)
                    )

                    cur.execute(sql, (fid, w_obs_id, airport_code, distance_km, is_at_risk))

        logger.info(f"Built {len(flight_ids)} flight-weather correlation records.")
    except Exception as e:
        logger.error(f"Correlation insert failed: {e}")
        raise
    finally:
        conn.close()