import os
import requests, csv
import logging
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

OUTPUT_SCHEMA = [
    "place",
    "city",
    "country",
    "weather",
    "temp_c",
    "humidity",
    "wind_speed",
]


# --- STEP 1: FETCH GEO COORDINATES ---
def fetch_geo_coor(city, retries=3):
    logging.info(f"City requested: {city}")

    url = f"https://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_KEY}"

    attempts = 0
    while attempts < retries:
        attempts += 1
        try:
            logging.info(f"Fetch geo coordinates API attempt {attempts}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if not data:
                raise ValueError(f"No geo coordinates for city: {city}")

            lat = data[0].get("lat")
            lon = data[0].get("lon")
            if lon is None or lat is None:
                raise ValueError("Invalid response: missing geo coordinates")

            logging.info("Fetch geo coordinates API success")
            return lat, lon
        except requests.exceptions.RequestException as e:
            logging.warning(f"API attempt {attempts} failed: {e}")
            if attempts == retries:
                logging.error(f"Max retries reached. Aborting.")
                raise


# --- STEP 2: FETCH WEATHER ---
def fetch_weather(lat, lon, retries=3) -> dict:
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"

    attempts = 0
    while attempts < retries:
        attempts += 1
        try:
            logging.info(f"Fetch weather API attempt {attempts}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("cod") != 200:
                raise ValueError(f"Weather API error: {data}")

            logging.info("Fetch weather API success")
            return data
        except requests.exceptions.RequestException as e:
            logging.warning(f"API attempt {attempts} failed: {e}")
            if attempts == retries:
                logging.error(f"Max retries reached. Aborting.")
                raise


# --- STEP 3: TRANSFORM ---
def transform_weather(raw_json: dict, city: str) -> dict:
    name = raw_json.get("name")

    # country
    sys = raw_json.get("sys", {})
    country = sys.get("country")

    # weather description
    weather_list = raw_json.get("weather", [{}])
    if weather_list:
        weather_desc = weather_list[0].get("description")

    # temperature
    main = raw_json.get("main", {})
    temp_k = main.get("temp")
    temp_c = round(temp_k - 273.15, 2) if temp_k else None

    # humidity
    humidity = main.get("humidity")

    # wind speed
    wind = raw_json.get("wind", {})
    wind_speed = wind.get("speed")

    data = {
        "place": name or city,
        "city": city,
        "country": country,
        "weather": weather_desc,
        "temp_c": temp_c,
        "humidity": humidity,
        "wind_speed": wind_speed,
    }

    logging.info(f"Transformed record: {data}")
    return data


# --- STEP 4: WRITE ---
def write_csv(data, fp="api_output.csv"):
    if not data:
        logging.warning(f"No valid data to write. Skipping CSV.")
        return

    with open(file=fp, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_SCHEMA)
        writer.writeheader()
        writer.writerow(data)

    logging.info(f"Output written to {fp}")


# --- MAIN ---
def main():
    city = input("Enter a city to get its weather info: ")
    lat, lon = fetch_geo_coor(city)
    raw_data = fetch_weather(lat, lon)
    data = transform_weather(raw_data, city)
    write_csv(data)


# --- ENTRY POINT ---
if __name__ == "__main__":
    try:
        main()
        print("SUCCESS")
    except Exception:
        logging.exception("Pipeline failed")
        print("FAILED")
