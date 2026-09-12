import os
import logging
from datetime import datetime, timezone

import requests
from pymongo import MongoClient


# ============================================================
# 1. LOGGING CONFIGURATION
# ============================================================

LOG_DIR = os.getenv(
    "COLLECTION_LOG_DIR",
    "/opt/airflow/logs/collection"
)

os.makedirs(LOG_DIR, exist_ok=True)

execution_time = datetime.now(
    timezone.utc
)

log_filename = (
    f"weather_collection_"
    f"{execution_time.strftime('%Y-%m-%d_%H%M%S')}.log"
)

log_file = os.path.join(
    LOG_DIR,
    log_filename
)


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "weather_collection"
)


# ============================================================
# 2. ENVIRONMENT VARIABLES
# ============================================================

API_KEY = os.getenv(
    "WEATHER_API_KEY"
)

MONGODB_URI = os.getenv(
    "MONGODB_URI"
)


if not API_KEY:

    logger.error(
        "WEATHER_API_KEY is not set"
    )

    raise ValueError(
        "WEATHER_API_KEY is not set"
    )


if not MONGODB_URI:

    logger.error(
        "MONGODB_URI is not set"
    )

    raise ValueError(
        "MONGODB_URI is not set"
    )


# ============================================================
# 3. CITY CONFIGURATION
# ============================================================

CITY = "GUNTUR"
COUNTRY = "IN"


logger.info(
    f"Starting weather collection | "
    f"City: {CITY} | Country: {COUNTRY}"
)


# ============================================================
# 4. MONGODB CONNECTION
# ============================================================

client = None

try:

    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=10000
    )

    client.admin.command("ping")

    logger.info(
        "MongoDB Atlas connection successful"
    )

    database = client["WeatherDB"]

    weather_collection = database[
        "weather_data"
    ]

    log_collection = database[
        "collection_logs"
    ]


except Exception as e:

    logger.exception(
        f"MongoDB connection failed: {e}"
    )

    raise


# ============================================================
# 5. LOG FUNCTION
# ============================================================

def save_log(
    level,
    message
):

    log_document = {

        "execution_id": log_filename,

        "execution_time": datetime.now(
            timezone.utc
        ),

        "city": CITY,

        "level": level,

        "message": message,

        "log_file": log_filename
    }

    try:

        log_collection.insert_one(
            log_document
        )

    except Exception as e:

        logger.error(
            f"Failed to save log to MongoDB: {e}"
        )


# ============================================================
# 6. OPENWEATHER API
# ============================================================

url = (
    "https://api.openweathermap.org/data/2.5/weather"
)

params = {

    "q": f"{CITY},{COUNTRY}",

    "appid": API_KEY,

    "units": "metric"
}


# ============================================================
# 7. API REQUEST
# ============================================================

try:

    logger.info(
        "Sending request to OpenWeather API"
    )

    save_log(
        "INFO",
        "Sending request to OpenWeather API"
    )


    response = requests.get(

        url,

        params=params,

        timeout=30
    )


    response.raise_for_status()


    data = response.json()


    logger.info(
        "Weather API request successful"
    )

    save_log(
        "INFO",
        "Weather API request successful"
    )


except requests.exceptions.RequestException as e:

    logger.exception(
        f"Weather API request failed: {e}"
    )

    save_log(
        "ERROR",
        f"Weather API request failed: {e}"
    )

    raise


# ============================================================
# 8. CREATE WEATHER RECORD
# ============================================================

try:

    weather_record = {

        "city": CITY,

        "country": COUNTRY,

        "latitude": data["coord"]["lat"],

        "longitude": data["coord"]["lon"],

        "temperature": data["main"]["temp"],

        "feels_like": data["main"]["feels_like"],

        "min_temperature": data["main"]["temp_min"],

        "max_temperature": data["main"]["temp_max"],

        "pressure": data["main"]["pressure"],

        "humidity": data["main"]["humidity"],

        "wind_speed": data["wind"]["speed"],

        "cloudiness": data["clouds"]["all"],

        "weather_condition": (
            data["weather"][0]["main"]
        ),

        "weather_description": (
            data["weather"][0]["description"]
        ),

        "collection_time": datetime.now(
            timezone.utc
        )
    }


    logger.info(
        "Weather record created successfully"
    )

    save_log(
        "INFO",
        "Weather record created successfully"
    )


except KeyError as e:

    logger.exception(
        f"Required API field missing: {e}"
    )

    save_log(
        "ERROR",
        f"Required API field missing: {e}"
    )

    raise


# ============================================================
# 9. INSERT WEATHER DATA
# ============================================================

try:

    result = weather_collection.insert_one(
        weather_record
    )


    logger.info(
        "Weather data inserted successfully | "
        f"Document ID: {result.inserted_id}"
    )


    save_log(
        "INFO",
        "Weather data inserted successfully"
    )


except Exception as e:

    logger.exception(
        f"Weather data insertion failed: {e}"
    )

    save_log(
        "ERROR",
        f"Weather data insertion failed: {e}"
    )

    raise


# ============================================================
# 10. COLLECTION SUMMARY
# ============================================================

logger.info(
    f"Collection completed | "
    f"City: {CITY} | "
    f"Temperature: {weather_record['temperature']}°C | "
    f"Humidity: {weather_record['humidity']}%"
)


save_log(
    "INFO",
    "Weather data collection completed successfully"
)


# ============================================================
# 11. FINAL LOG
# ============================================================

logger.info(
    f"Execution completed successfully | "
    f"Log file: {log_filename}"
)


save_log(
    "INFO",
    f"Execution completed successfully | "
    f"Log file: {log_filename}"
)


# ============================================================
# 12. CLOSE MONGODB
# ============================================================

if client:

    client.close()

    logger.info(
        "MongoDB connection closed"
    )