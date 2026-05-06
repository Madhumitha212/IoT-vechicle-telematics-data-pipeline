from config.logger import logger
from src.redshift.execute_query import run_redshift_query
from config.get_client import BASE_S3, REDSHIFT_IAM_ROLE_ARN

def create_tables():
    create_sql = [
        """
        CREATE TABLE IF NOT EXISTS vehicle_metrics (
            deviceID BIGINT,
            avg_speed FLOAT,
            avg_temp FLOAT,
            avg_battery FLOAT,
            anomaly_count BIGINT,
            fault_count BIGINT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS trip_metrics (
            tripID BIGINT,
            avg_speed FLOAT,
            max_temp FLOAT,
            trip_duration BIGINT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS anomaly_summary (
            deviceID BIGINT,
            anomaly_type VARCHAR(50),
            count BIGINT
        );
        """
    ]
    log = logger.bind(stage="create_tables")
    log.info("Creating tables...")
    for sql in create_sql:
        run_redshift_query(sql)
    log.success("Tables created successfully")

def load_data():
    load_vehicle = f"""
        COPY vehicle_metrics
        FROM '{BASE_S3}/aggregated/device/'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """
    load_trip = f"""
        COPY trip_metrics
        FROM '{BASE_S3}/aggregated/trip/'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """
    load_anomaly = f"""
        COPY anomaly_summary
        FROM '{BASE_S3}/aggregated/anomaly_summary/'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """

    log = logger.bind(stage="load_data")
    log.info("Loading data into Redshift...")
    run_redshift_query(load_vehicle)
    run_redshift_query(load_trip)
    run_redshift_query(load_anomaly)
    log.success("Data loaded into Redshift")
