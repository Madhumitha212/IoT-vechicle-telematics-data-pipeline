from pyspark.sql import SparkSession

from src.data_processing.data_cleaning import clean_data
from src.data_processing.time_series import add_window_features
from src.data_processing.anamoly_detection import detect_anomalies
from src.data_processing.analytics import run_aggregations

from config.logger import logger


# ------------------ SPARK SESSION ------------------ #
def create_session(app_name: str):

    log = logger.bind(stage="spark")

    log.info("Creating Spark session...")

    spark = (
        SparkSession.builder
        .appName(app_name)
        # --- AWS S3 connector ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # --- Memory & GC tuning ---
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        # --- Parquet write tuning ---
        .config("spark.sql.parquet.rowGroupSize", str(64 * 1024 * 1024))  # 64 MB
        .config("spark.sql.files.maxConcurrentWrites", "2")
        .config("spark.sql.shuffle.partitions", "400")
        # --- Heartbeat & network ---
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.files.maxConcurrentWrites", "4")
        .getOrCreate()
    )
       

    log.info("Spark session created successfully")
    return spark


# ------------------ LOAD DATA ------------------ #
def load_data(spark, path):

    log = logger.bind(stage="load")

    log.info(f"Loading raw data from {path}...")

    df = spark.read.json(path)

    log.info("Data loaded successfully")
    return df


# ------------------ SAVE DATA ------------------ #
def save_data(df, base_path, folders, partition_cols=None):

    log = logger.bind(stage="save")

    full_path = "/".join([base_path.rstrip("/")] + folders)

    log.info(f"Saving data to {full_path}...")

    df = df.coalesce(100)

    writer = df.write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(full_path)

    log.info("Data saved successfully")


# ------------------ MAIN PIPELINE ------------------ #
def main():

    log = logger.bind(stage="pipeline")

    log.info("IoT Vehicle Data Pipeline Started")

    base_path = "s3a://vehicle-telematics-bucket"

    spark = create_session("Vehicle Telematics Pipeline")

    try:
        # STEP 1: LOAD
        df = load_data(spark, f"{base_path}/raw/")

        # STEP 2: CLEANING
        log.info("Starting data cleaning...")
        df = clean_data(df, save_data, base_path)
        log.info("Data cleaning completed")

        # STEP 3: TIME SERIES
        log.info("Starting time-series feature engineering...")
        df = add_window_features(df, save_data, base_path)
        log.info("Time-series features completed")

        # STEP 4: ANOMALY
        log.info("Starting anomaly detection...")
        df = detect_anomalies(df, save_data, base_path)
        log.info("Anomaly detection completed")

        # STEP 5: AGGREGATION
        log.info("Starting aggregations...")
        agg_results = run_aggregations(df, save_data, base_path)
        log.info("Aggregations completed")

        log.info("Pipeline completed successfully")

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)

    finally:
        spark.stop()
        log.info("Spark session stopped")


if __name__ == "__main__":
    main()