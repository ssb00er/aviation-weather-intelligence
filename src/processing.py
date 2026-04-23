"""
processing.py
-------------
Cleans, enriches and validates raw data returned by ingestion.py.
Adds derived fields (severity scores, altitude categories, nearest airport)
before the data is passed to storage.py for insertion into PostgreSQL.
"""

import math
import logging
import pandas as pd
from datetime import timezone

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  WEATHER SEVERITY THRESHOLDS
#  Based on ICAO and Indian Meteorological Department (IMD) standards
# --------------------------------------------------------------------------- #
SEVERITY_RULES = {
    # wind_speed_ms thresholds
    "wind_moderate":  10.0,   # 36 km/h
    "wind_severe":    17.5,   # 63 km/h — gale force
    "wind_extreme":   24.5,   # 88 km/h — storm force

    # visibility_m thresholds
    "vis_low":        5000,   # Reduced visibility
    "vis_poor":       1500,   # Poor visibility
    "vis_fog":         500,   # Fog/near zero

    # Hazardous weather_main codes
    "hazardous_conditions": ["Thunderstorm", "Tornado", "Squall"],
    "bad_conditions":       ["Snow", "Sleet", "Hail"],
}


def calculate_severity_score(weather: dict) -> tuple[int, str, str]:
    """
    Calculates a composite severity score (0-100) for a weather observation.
    Also returns a severity label and alert event description.

    Scoring logic:
      - Wind speed:   up to 40 points
      - Visibility:   up to 30 points
      - Weather type: up to 30 points

    Returns: (score, severity_label, alert_event)
    """
    score = 0
    alert_event = None

    wind   = weather.get("wind_speed_ms") or 0
    vis    = weather.get("visibility_m")  or 10000
    w_main = weather.get("weather_main")  or ""

    # --- Wind scoring ---
    if wind >= SEVERITY_RULES["wind_extreme"]:
        score += 40
        alert_event = "Extreme Wind"
    elif wind >= SEVERITY_RULES["wind_severe"]:
        score += 25
        alert_event = "Severe Wind"
    elif wind >= SEVERITY_RULES["wind_moderate"]:
        score += 10

    # --- Visibility scoring ---
    if vis <= SEVERITY_RULES["vis_fog"]:
        score += 30
        alert_event = alert_event or "Dense Fog"
    elif vis <= SEVERITY_RULES["vis_poor"]:
        score += 20
        alert_event = alert_event or "Poor Visibility"
    elif vis <= SEVERITY_RULES["vis_low"]:
        score += 10

    # --- Weather condition scoring ---
    if w_main in SEVERITY_RULES["hazardous_conditions"]:
        score += 30
        alert_event = alert_event or w_main
    elif w_main in SEVERITY_RULES["bad_conditions"]:
        score += 15
        alert_event = alert_event or w_main

    # --- Map score to label ---
    if score >= 60:
        severity_label = "extreme"
    elif score >= 35:
        severity_label = "severe"
    elif score >= 15:
        severity_label = "moderate"
    else:
        severity_label = "low"

    return score, severity_label, alert_event


def enrich_weather(weather_list: list[dict]) -> list[dict]:
    """
    Takes raw weather records from ingestion.py and adds:
    - severity_score
    - alert_severity label
    - has_alert flag
    - alert_event description
    """
    enriched = []
    for w in weather_list:
        score, label, event = calculate_severity_score(w)
        w["severity_score"] = score
        w["alert_severity"] = label
        w["has_alert"]      = score >= 15     # Anything above 'low' triggers alert
        w["alert_event"]    = event
        enriched.append(w)

    severe_count = sum(1 for w in enriched if w["has_alert"])
    logger.info(f"Weather enrichment complete. {severe_count}/{len(enriched)} airports have active alerts.")
    return enriched


def categorize_altitude(altitude_m: float) -> str:
    """
    Categorizes flight altitude into human-readable bands.
    Based on standard aviation altitude classifications.
    """
    if altitude_m is None:
        return "unknown"
    elif altitude_m < 3000:
        return "low"           # Below 10,000 ft — approach/departure
    elif altitude_m < 7500:
        return "mid"           # 10,000–25,000 ft — transition
    else:
        return "cruise"        # Above 25,000 ft — en-route cruise


def categorize_speed(velocity_ms: float) -> str:
    """
    Categorizes aircraft speed into human-readable bands.
    """
    if velocity_ms is None:
        return "unknown"
    elif velocity_ms < 100:
        return "slow"          # < 360 km/h — prop aircraft or approach
    elif velocity_ms < 250:
        return "normal"        # 360–900 km/h — typical jet cruise
    else:
        return "fast"          # > 900 km/h — supersonic / data anomaly


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculates the great-circle distance between two coordinates in kilometres.
    Used to find which monitored airport each flight is closest to.
    """
    R = 6371  # Earth's radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi        = math.radians(lat2 - lat1)
    dlambda     = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Airport coordinates keyed by code — mirrors our DB airports table
AIRPORT_COORDS = {
    "DEL": (28.5562,  77.1000),
    "BOM": (19.0896,  72.8656),
    "MAA": (12.9941,  80.1709),
    "HYD": (17.2403,  78.4294),
    "BLR": (13.1986,  77.7066),
    "CCU": (22.6520,  88.4463),
    "AMD": (23.8433,  72.6347),
    "JAI": (26.8242,  75.8122),
    "KHI": (24.6962,  67.1609),
    "DAC": (23.7647,  90.3784),
    "CMB": ( 7.1800,  79.8847),
    "KTM": (27.6966,  85.3591),
}


def find_nearest_airport(lat: float, lon: float) -> tuple[str, float]:
    """
    Finds the nearest monitored airport to a given coordinate.
    Returns (airport_code, distance_km).
    """
    nearest_code = None
    nearest_dist = float("inf")

    for code, (a_lat, a_lon) in AIRPORT_COORDS.items():
        dist = haversine_km(lat, lon, a_lat, a_lon)
        if dist < nearest_dist:
            nearest_dist = dist
            nearest_code = code

    return nearest_code, round(nearest_dist, 2)


def enrich_flights(flight_list: list[dict]) -> list[dict]:
    """
    Takes raw flight records from ingestion.py and adds:
    - altitude_category
    - speed_category
    - nearest_airport (code)
    - distance_to_nearest_airport_km
    """
    enriched = []
    for f in flight_list:
        f["altitude_category"] = categorize_altitude(f.get("altitude_m"))
        f["speed_category"]    = categorize_speed(f.get("velocity_ms"))

        if f.get("latitude") and f.get("longitude"):
            code, dist = find_nearest_airport(f["latitude"], f["longitude"])
            f["nearest_airport"]              = code
            f["distance_to_nearest_airport_km"] = dist
        else:
            f["nearest_airport"]              = None
            f["distance_to_nearest_airport_km"] = None

        enriched.append(f)

    logger.info(f"Flight enrichment complete. Processed {len(enriched)} flights.")
    return enriched


# --------------------------------------------------------------------------- #
#  QUICK TEST
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # Simulate a single raw weather record as ingestion.py would return it
    sample_weather = [{
        "airport_name":  "DEL - Delhi Indira Gandhi",
        "latitude":      28.5562,
        "longitude":     77.1000,
        "temp_c":        38.5,
        "wind_speed_ms": 18.0,
        "wind_gust_ms":  None,
        "visibility_m":  4000,
        "weather_main":  "Haze",
        "weather_desc":  "haze",
        "weather_id":    721,
        "humidity_pct":  25,
        "pressure_hpa":  1005.0,
        "cloud_pct":     20,
        "has_alert":     False,
        "alert_event":   None,
        "alert_severity": None,
    }]

    sample_flight = [{
        "icao24":        "801641",
        "callsign":      "AXB1408",
        "latitude":      21.4658,
        "longitude":     84.5432,
        "altitude_m":    11582.4,
        "velocity_ms":   202.75,
        "on_ground":     False,
    }]

    print("\n--- Enriched Weather ---")
    enriched_w = enrich_weather(sample_weather)
    for k, v in enriched_w[0].items():
        print(f"  {k}: {v}")

    print("\n--- Enriched Flight ---")
    enriched_f = enrich_flights(sample_flight)
    for k, v in enriched_f[0].items():
        print(f"  {k}: {v}")