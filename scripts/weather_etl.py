import os
import logging
from datetime import datetime, timezone

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    lit
)


# ============================================================
# 1. LOG FILE CONFIGURATION
# ============================================================

LOG_DIR = os.getenv(
    "ETL_LOG_DIR",
    "/opt/airflow/logs/etl"
)

os.makedirs(
    LOG_DIR,
    exist_ok=True
)

execution_time = datetime.now(
    timezone.utc
)

log_filename = (
    f"weather_etl_"
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
    "weather_etl"
)


# ============================================================
# 2. ENVIRONMENT VARIABLES
# ============================================================

MONGODB_URI = os.getenv(
    "MONGODB_URI"
)

DATABASE_URL = os.getenv(
    "DATABASE_URL"
)


if not MONGODB_URI:

    logger.error(
        "MONGODB_URI is not set"
    )

    raise ValueError(
        "MONGODB_URI is not set"
    )


if not DATABASE_URL:

    logger.error(
        "DATABASE_URL is not set"
    )

    raise ValueError(
        "DATABASE_URL is not set"
    )


# ============================================================
# 3. EXECUTION INFORMATION
# ============================================================

EXECUTION_ID = log_filename

CITY = "GUNTUR"

logger.info("=" * 60)

logger.info(
    "WEATHER ETL STARTED"
)

logger.info(
    f"Execution ID: {EXECUTION_ID}"
)

logger.info("=" * 60)


# ============================================================
# 4. MONGODB CONNECTION FOR LOGGING
# ============================================================

mongo_client = None

try:

    logger.info(
        "Connecting to MongoDB Atlas for ETL logging"
    )

    mongo_client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=10000
    )

    mongo_client.admin.command(
        "ping"
    )

    mongo_database = mongo_client[
        "WeatherDB"
    ]

    weather_collection = mongo_database[
        "weather_data"
    ]

    log_collection = mongo_database[
        "etl_logs"
    ]

    logger.info(
        "MongoDB Atlas connection successful"
    )

except Exception as e:

    logger.exception(
        f"MongoDB connection failed: {e}"
    )

    raise


# ============================================================
# 5. SAVE LOG TO MONGODB
# ============================================================

def save_log(
    level,
    message
):

    log_document = {

        "execution_id": EXECUTION_ID,

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

        # Do not stop ETL just because log insertion failed
        logger.error(
            f"Failed to save ETL log to MongoDB: {e}"
        )


# ============================================================
# 6. LOG START
# ============================================================

save_log(
    "INFO",
    "WEATHER ETL STARTED"
)


# ============================================================
# 7. SPARK SESSION
# ============================================================

logger.info(
    "Creating Spark session"
)

save_log(
    "INFO",
    "Creating Spark session"
)


spark = None


try:

    spark = (
        SparkSession.builder
        .appName(
            "Weather MongoDB to Supabase PostgreSQL ETL"
        )
        .config(
            "spark.mongodb.read.connection.uri",
            MONGODB_URI
        )
        .getOrCreate()
    )


    logger.info(
        "Spark session created successfully"
    )

    save_log(
        "INFO",
        "Spark session created successfully"
    )


    # ========================================================
    # 8. READ MONGODB WEATHER DATA
    # ========================================================

    logger.info(
        "Reading weather data from MongoDB Atlas"
    )

    save_log(
        "INFO",
        "Reading weather data from MongoDB Atlas"
    )


    df = (
        spark.read
        .format("mongodb")
        .option(
            "database",
            "WeatherDB"
        )
        .option(
            "collection",
            "weather_data"
        )
        .load()
    )


    mongo_count = df.count()


    logger.info(
        f"Records read from MongoDB: {mongo_count}"
    )

    save_log(
        "INFO",
        f"Records read from MongoDB: {mongo_count}"
    )


    # ========================================================
    # 9. SELECT / FLATTEN FIELDS
    # ========================================================

    logger.info(
        "Starting JSON flattening and transformation"
    )

    save_log(
        "INFO",
        "Starting JSON flattening and transformation"
    )


    weather_df = df.select(

        col("city").alias(
            "city"
        ),

        col("country").alias(
            "country"
        ),

        col("latitude")
        .cast("double")
        .alias("latitude"),

        col("longitude")
        .cast("double")
        .alias("longitude"),

        col("temperature")
        .cast("double")
        .alias("temperature"),

        col("feels_like")
        .cast("double")
        .alias("feels_like"),

        col("min_temperature")
        .cast("double")
        .alias("min_temperature"),

        col("max_temperature")
        .cast("double")
        .alias("max_temperature"),

        col("pressure")
        .cast("double")
        .alias("pressure"),

        col("humidity")
        .cast("double")
        .alias("humidity"),

        col("wind_speed")
        .cast("double")
        .alias("wind_speed"),

        col("cloudiness")
        .cast("double")
        .alias("cloudiness"),

        col("weather_condition")
        .alias("weather_condition"),

        col("weather_description")
        .alias("weather_description"),

        col("collection_time")
        .alias("collection_time")
    )


    logger.info(
        "JSON flattening completed"
    )

    save_log(
        "INFO",
        "JSON flattening completed"
    )


    # ========================================================
    # 10. CONVERT COLLECTION TIME
    # ========================================================

    logger.info(
        "Converting collection_time to timestamp"
    )

    save_log(
        "INFO",
        "Converting collection_time to timestamp"
    )


    weather_df = weather_df.withColumn(
        "collection_time",
        to_timestamp(
            col("collection_time")
        )
    )


    # ========================================================
    # 11. CREATE COLLECTION DATE
    # ========================================================

    weather_df = weather_df.withColumn(
        "collection_date",
        to_date(
            col("collection_time")
        )
    )


    logger.info(
        "Collection date created"
    )

    save_log(
        "INFO",
        "Collection date created"
    )


    # ========================================================
    # 12. GET TODAY'S DATE
    # ========================================================

    today = spark.sql(
        "SELECT current_date()"
    ).collect()[0][0]


    logger.info(
        f"Processing date: {today}"
    )

    save_log(
        "INFO",
        f"Processing date: {today}"
    )


    # ========================================================
    # 13. FILTER TODAY'S RECORDS
    # ========================================================

    weather_today = weather_df.filter(

        col("collection_date")
        == lit(today)
    )


    today_count_before_duplicate = (
        weather_today.count()
    )


    logger.info(
        "Today's records before duplicate "
        f"removal: {today_count_before_duplicate}"
    )

    save_log(
        "INFO",
        "Today's records before duplicate "
        f"removal: {today_count_before_duplicate}"
    )


    # ========================================================
    # 14. REMOVE DUPLICATES
    # ========================================================

    weather_today = weather_today.dropDuplicates(

        [
            "city",
            "collection_time"
        ]
    )


    # ========================================================
    # 15. FINAL RECORD COUNT
    # ========================================================

    record_count = weather_today.count()


    duplicates_removed = (
        today_count_before_duplicate
        - record_count
    )


    logger.info(
        f"Duplicates removed: {duplicates_removed}"
    )

    save_log(
        "INFO",
        f"Duplicates removed: {duplicates_removed}"
    )


    logger.info(
        f"Final records for PostgreSQL: "
        f"{record_count}"
    )

    save_log(
        "INFO",
        f"Final records for PostgreSQL: "
        f"{record_count}"
    )


    # ========================================================
    # 16. EXPECTED RECORD VALIDATION
    # ========================================================

    expected_records = 144


    if record_count == expected_records:

        message = (
            f"Record validation PASSED: "
            f"{record_count}/{expected_records}"
        )

        logger.info(
            message
        )

        save_log(
            "INFO",
            message
        )


    elif record_count < expected_records:

        missing_records = (
            expected_records
            - record_count
        )

        message = (
            f"Record validation WARNING: "
            f"Expected {expected_records}, "
            f"but found {record_count}. "
            f"Missing {missing_records} records."
        )

        logger.warning(
            message
        )

        save_log(
            "WARNING",
            message
        )


    else:

        extra_records = (
            record_count
            - expected_records
        )

        message = (
            f"Record validation WARNING: "
            f"Expected {expected_records}, "
            f"but found {record_count}. "
            f"{extra_records} extra records found."
        )

        logger.warning(
            message
        )

        save_log(
            "WARNING",
            message
        )


    # ========================================================
    # 17. SUPABASE POSTGRESQL CONFIGURATION
    # ========================================================

    logger.info(
        "Configuring Supabase PostgreSQL connection"
    )

    save_log(
        "INFO",
        "Configuring Supabase PostgreSQL connection"
    )


    postgres_properties = {

        "driver":
            "org.postgresql.Driver"
    }


    # ========================================================
    # 18. LOAD DATA INTO SUPABASE
    # ========================================================

    if record_count > 0:

        logger.info(
            "Starting Supabase PostgreSQL load"
        )

        save_log(
            "INFO",
            "Starting Supabase PostgreSQL load"
        )


        (
            weather_today.write
            .mode("append")
            .jdbc(
                url=DATABASE_URL,
                table="weather_data",
                properties=postgres_properties
            )
        )


        logger.info(
            f"Successfully inserted "
            f"{record_count} records into "
            f"Supabase PostgreSQL"
        )

        save_log(
            "INFO",
            f"Successfully inserted "
            f"{record_count} records into "
            f"Supabase PostgreSQL"
        )


    else:

        message = (
            "No records found for today's date. "
            "Nothing was inserted into "
            "Supabase PostgreSQL."
        )

        logger.warning(
            message
        )

        save_log(
            "WARNING",
            message
        )


    # ========================================================
    # 19. ETL COMPLETED
    # ========================================================

    logger.info(
        "WEATHER ETL COMPLETED SUCCESSFULLY"
    )

    save_log(
        "INFO",
        "WEATHER ETL COMPLETED SUCCESSFULLY"
    )


except Exception as e:

    logger.exception(
        f"WEATHER ETL FAILED: {e}"
    )

    save_log(
        "ERROR",
        f"WEATHER ETL FAILED: {e}"
    )

    raise


finally:

    # ========================================================
    # 20. STOP SPARK
    # ========================================================

    if spark:

        spark.stop()

        logger.info(
            "Spark session stopped"
        )

        save_log(
            "INFO",
            "Spark session stopped"
        )


    # ========================================================
    # 21. CLOSE MONGODB
    # ========================================================

    if mongo_client:

        mongo_client.close()

        logger.info(
            "MongoDB connection closed"
        )


    logger.info("=" * 60)

    logger.info(
        "ETL EXECUTION FINISHED"
    )

    logger.info("=" * 60)