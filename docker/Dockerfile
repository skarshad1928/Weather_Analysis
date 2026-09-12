FROM apache/airflow:3.0.2

USER root

# ==========================================================
# SYSTEM DEPENDENCIES
# ==========================================================

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# ==========================================================
# JAVA
# ==========================================================

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


# ==========================================================
# SPARK / PYTHON DEPENDENCIES
# ==========================================================

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark==3.5.3 \
    pandas \
    numpy \
    requests \
    python-dotenv \
    pymongo \
    psycopg2-binary


# ==========================================================
# SPARK JAR DIRECTORY
# ==========================================================

USER root

RUN mkdir -p /opt/spark/jars


# ==========================================================
# POSTGRESQL JDBC DRIVER
# ==========================================================

RUN curl -L \
    -o /opt/spark/jars/postgresql-42.7.5.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.5.jar


# ==========================================================
# MONGODB SPARK CONNECTOR
# Compatible with Spark 3.5.x
# ==========================================================

RUN curl -L \
    -o /opt/spark/jars/mongo-spark-connector_2.12-10.4.0.jar \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-spark-connector_2.12/10.4.0/mongodb-spark-connector_2.12-10.4.0.jar


# ==========================================================
# ENVIRONMENT
# ==========================================================

ENV SPARK_HOME=/opt/spark

ENV SPARK_JARS="/opt/spark/jars/postgresql-42.7.5.jar,/opt/spark/jars/mongo-spark-connector_2.12-10.4.0.jar"

ENV PATH="${SPARK_HOME}/bin:${PATH}"


# ==========================================================
# WORKING DIRECTORY
# ==========================================================

WORKDIR /opt/airflow


# ==========================================================
# DIRECTORIES
# ==========================================================

RUN mkdir -p \
    /opt/airflow/logs/collection \
    /opt/airflow/logs/etl \
    /opt/airflow/data \
    /opt/airflow/scripts \
    /opt/airflow/dags

RUN chown -R airflow:root /opt/airflow/logs \
    /opt/airflow/data \
    /opt/airflow/scripts \
    /opt/airflow/dags


# ==========================================================
# FINAL USER
# ==========================================================

USER airflow