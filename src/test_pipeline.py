import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(levelname)s]  %(message)s")

from ingestion import fetch_live_flights, fetch_all_airport_weather
from processing import enrich_flights, enrich_weather
from storage import insert_flight_telemetry, insert_weather_observations, insert_correlations

print("\n--- Fetching & Enriching Flights ---")
flights = enrich_flights(fetch_live_flights())

print("\n--- Fetching & Enriching Weather ---")
weather = enrich_weather(fetch_all_airport_weather())

print("\n--- Inserting into PostgreSQL ---")
w_ids = insert_weather_observations(weather)
f_ids = insert_flight_telemetry(flights)

weather_map = {
    w["airport_code"]: {
        "id":             wid,
        "severity_score": w["severity_score"],
        "has_alert":      w["has_alert"]
    }
    for w, wid in zip(weather, w_ids)
}

insert_correlations(f_ids, flights, weather_map)

print("\n✓ Pipeline test complete.")