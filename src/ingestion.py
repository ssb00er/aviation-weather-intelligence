"""
ingestion.py
------------
Handles all API communication with OpenSky Network and OpenWeatherMap.
Fetches raw flight telemetry and weather alert data.
"""

import requests
import logging
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# --------------------------------------------------------------------------- #
#  Logging setup — writes to console so we can see what's happening live
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#  CONSTANTS
# --------------------------------------------------------------------------- #
OPENSKY_BASE_URL     = "https://opensky-network.org/api"
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"

# Geographic bounding box — Indian Subcontinent
# Covers India, Pakistan, Bangladesh, Sri Lanka, Nepal, Myanmar
BOUNDING_BOX = {
    "min_lat":  6.0,
    "max_lat": 37.0,
    "min_lon": 61.0,
    "max_lon": 97.0
}

# Major airports across the Indian Subcontinent
MONITORED_AIRPORTS = [
    (28.5562,  77.1000,  "DEL - Delhi Indira Gandhi"),
    (19.0896,  72.8656,  "BOM - Mumbai Chhatrapati Shivaji"),
    (12.9941,  80.1709,  "MAA - Chennai"),
    (17.2403,  78.4294,  "HYD - Hyderabad Rajiv Gandhi"),
    (13.1986,  77.7066,  "BLR - Bengaluru Kempegowda"),
    (22.6520,  88.4463,  "CCU - Kolkata Netaji Subhas"),
    (23.8433,  72.6347,  "AMD - Ahmedabad Sardar Vallabhbhai"),
    (26.8242,  75.8122,  "JAI - Jaipur International"),
    (24.6962,  67.1609,  "KHI - Karachi Jinnah"),           # Pakistan
    (23.7647,  90.3784,  "DAC - Dhaka Hazrat Shahjalal"),   # Bangladesh
    ( 7.1800,  79.8847,  "CMB - Colombo Bandaranaike"),     # Sri Lanka
    (27.6966,  85.3591,  "KTM - Kathmandu Tribhuvan"),      # Nepal
]


# --------------------------------------------------------------------------- #
#  OPENSKY NETWORK — Flight Telemetry
# --------------------------------------------------------------------------- #

def fetch_live_flights() -> list[dict]:
    """
    Fetches all live aircraft currently in the air within our bounding box.
    Returns a list of cleaned flight dictionaries.

    OpenSky returns each flight as a positional list (not a dict), so we
    map fields manually by index — as per their official API spec:
    https://openskynetwork.github.io/opensky-api/rest.html
    """
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")

    params = {
        "lamin": BOUNDING_BOX["min_lat"],
        "lamax": BOUNDING_BOX["max_lat"],
        "lomin": BOUNDING_BOX["min_lon"],
        "lomax": BOUNDING_BOX["max_lon"],
    }

    logger.info("Fetching live flight data from OpenSky Network...")

    try:
        response = requests.get(
            f"{OPENSKY_BASE_URL}/states/all",
            params=params,
            auth=(username, password),
            timeout=30
        )
        response.raise_for_status()
        raw = response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f"OpenSky HTTP error: {e.response.status_code} — {e.response.text}")
        return []
    except requests.exceptions.ConnectionError:
        logger.error("OpenSky: Could not connect. Check your internet connection.")
        return []
    except requests.exceptions.Timeout:
        logger.error("OpenSky: Request timed out after 30s.")
        return []

    if not raw or "states" not in raw or raw["states"] is None:
        logger.warning("OpenSky returned no active flights in bounding box.")
        return []

    flights = []
    for state in raw["states"]:
        flight = {
            "icao24":           state[0],
            "callsign":         str(state[1]).strip() if state[1] else None,
            "origin_country":   state[2],
            "timestamp_utc":    datetime.fromtimestamp(state[3], tz=timezone.utc) if state[3] else None,
            "last_contact_utc": datetime.fromtimestamp(state[4], tz=timezone.utc) if state[4] else None,
            "longitude":        state[5],
            "latitude":         state[6],
            "altitude_m":       state[7],   # Barometric altitude in metres
            "on_ground":        state[8],
            "velocity_ms":      state[9],   # Speed in m/s
            "heading_deg":      state[10],  # Track angle in degrees
            "vertical_rate_ms": state[11],  # Climb/descent rate in m/s
            "geo_altitude_m":   state[13],  # GPS altitude in metres
            "squawk":           state[14],  # Transponder squawk code
        }

        # Skip aircraft on the ground or with no position fix
        if flight["on_ground"] or flight["latitude"] is None or flight["longitude"] is None:
            continue

        flights.append(flight)

    logger.info(f"OpenSky: Retrieved {len(flights)} airborne flights.")
    return flights


# --------------------------------------------------------------------------- #
#  OPENWEATHERMAP — Current Weather (2.5 free tier)
# --------------------------------------------------------------------------- #

def fetch_weather_for_airport(lat: float, lon: float, airport_name: str) -> dict:
    """
    Fetches current weather conditions for a given airport coordinate
    using the OpenWeatherMap 2.5 /weather endpoint.

    NOTE: The 2.5 free tier returns data at the TOP LEVEL of the response
    (not nested under 'current' like the 3.0 One Call API).
    Response structure reference:
    https://openweathermap.org/current#example_JSON

    Since the free tier has no native alert field, we derive a severity
    flag ourselves in processing.py based on wind speed and visibility.
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")

    params = {
        "lat":   lat,
        "lon":   lon,
        "appid": api_key,
        "units": "metric",   # Celsius, m/s
    }

    try:
        response = requests.get(
            f"{OPENWEATHER_BASE_URL}/weather",
            params=params,
            timeout=15
        )
        response.raise_for_status()
        raw = response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f"OpenWeatherMap error for {airport_name}: {e.response.status_code}")
        return {}
    except requests.exceptions.Timeout:
        logger.error(f"OpenWeatherMap: Timed out for {airport_name}.")
        return {}
    except requests.exceptions.ConnectionError:
        logger.error(f"OpenWeatherMap: Connection error for {airport_name}.")
        return {}

    # ------------------------------------------------------------------
    # 2.5 /weather response is FLAT — fields sit directly on `raw`,
    # with sub-dicts for "main", "wind", "weather", etc.
    # ------------------------------------------------------------------
    weather_data = {
        "airport_name":  airport_name,
        "latitude":      lat,
        "longitude":     lon,

        # Top-level timestamp
        "timestamp_utc": datetime.fromtimestamp(raw.get("dt", 0), tz=timezone.utc),

        # Nested under "main"
        "temp_c":        raw.get("main", {}).get("temp"),
        "feels_like_c":  raw.get("main", {}).get("feels_like"),
        "humidity_pct":  raw.get("main", {}).get("humidity"),
        "pressure_hpa":  raw.get("main", {}).get("pressure"),

        # Nested under "wind"
        "wind_speed_ms": raw.get("wind", {}).get("speed"),
        "wind_gust_ms":  raw.get("wind", {}).get("gust"),   # Not always present
        "wind_deg":      raw.get("wind", {}).get("deg"),

        # Top-level visibility (in metres)
        "visibility_m":  raw.get("visibility"),

        # Nested under "weather" (a list — we take the first element)
        "weather_main":  raw.get("weather", [{}])[0].get("main"),
        "weather_desc":  raw.get("weather", [{}])[0].get("description"),
        "weather_id":    raw.get("weather", [{}])[0].get("id"),

        # Nested under "clouds"
        "cloud_pct":     raw.get("clouds", {}).get("all"),

        # Alerts derived in processing.py — placeholders here
        "has_alert":     False,
        "alert_event":   None,
        "alert_severity": None,
    }

    logger.info(
        f"Weather [{airport_name}]: {weather_data['weather_desc']} | "
        f"{weather_data['temp_c']}°C | "
        f"Wind: {weather_data['wind_speed_ms']} m/s | "
        f"Visibility: {weather_data['visibility_m']} m"
    )
    return weather_data


def fetch_all_airport_weather() -> list[dict]:
    """
    Loops through all monitored airports and fetches weather for each.
    Includes a 500ms delay between calls to respect rate limits.
    Returns a list of weather dictionaries.
    """
    logger.info(f"Fetching weather for {len(MONITORED_AIRPORTS)} monitored airports...")
    results = []

    for lat, lon, name in MONITORED_AIRPORTS:
        data = fetch_weather_for_airport(lat, lon, name)
        if data:
            results.append(data)
        time.sleep(0.5)  # Polite API usage

    logger.info(f"Weather fetch complete. Got data for {len(results)} airports.")
    return results


# --------------------------------------------------------------------------- #
#  QUICK TEST — run this file directly to verify both APIs are working
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  TESTING INGESTION PIPELINE")
    print("=" * 60)

    print("\n--- FLIGHT DATA ---")
    flights = fetch_live_flights()
    if flights:
        print(f"Sample flight: {flights[0]}")

    print("\n--- WEATHER DATA ---")
    weather = fetch_all_airport_weather()
    if weather:
        print(f"\nSample weather: {weather[0]}")

    print("\n" + "=" * 60)
    print("  INGESTION TEST COMPLETE")
    print("=" * 60)