from pyspark.sql.functions import *
from config.logger import logger

def run_aggregations(df,save_fn,  base_path):

    log = logger.bind(stage="aggregation")
    log.info("Starting aggregations")

    df = df.repartition("tripID")

    # ---------------- TRIP LEVEL ---------------- #
    df_trip = df.groupBy("tripID").agg(
        avg("gps_speed").alias("avg_speed"),
        max("cTemp").alias("max_temp"),
        (unix_timestamp(max("timeStamp")) -
         unix_timestamp(min("timeStamp"))).alias("trip_duration")
    )

    save_fn(df_trip, base_path, ["aggregated", "trip"], None)
    log.info("Trip aggregation completed")

    df = df.repartition("deviceID")
    # ---------------- DEVICE LEVEL ---------------- #
    df_device = df.groupBy("deviceID").agg(
        count("*").alias("total_records"),
        avg("gps_speed").alias("avg_speed"),
        avg("battery").alias("avg_battery"),
        sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        sum(col("fault_flag_final")).alias("fault_count")
    )

    save_fn(df_device, base_path, ["aggregated", "device"], None)
    log.info("Device aggregation completed")

    df = df.repartition("year", "month", "day", "hour")
    # ---------------- TIME BASED ---------------- #
    df_hour = df.groupBy("year", "month", "day", "hour").agg(
        avg("gps_speed").alias("avg_speed")
    )

    df_day = df.groupBy("year", "month", "day").agg(
        avg("cTemp").alias("avg_temp")
    )

    save_fn(df_hour, base_path, ["aggregated", "time_hour"], ["year","month","day"])
    save_fn(df_day, base_path, ["aggregated", "time_day"], ["year","month","day"])

    log.info("Time-based aggregation completed")

    # ---------------- CROSS METRICS ---------------- #
    corr_speed_load = df.stat.corr("gps_speed", "eLoad")
    corr_temp_load = df.stat.corr("cTemp", "eLoad")

    log.info(f"Correlation (speed vs load): {corr_speed_load}")
    log.info(f"Correlation (temp vs load): {corr_temp_load}")

    # ---------------- ANOMALY SUMMARY ---------------- #
    df_anomaly_summary = df.select(
        "deviceID",
        explode(array(
            when(col("overheat_flag") == 1, "OVERHEAT"),
            when(col("high_load_flag") == 1, "HIGH_LOAD"),
            when(col("speed_spike_flag") == 1, "SPEED_SPIKE"),
            when(col("battery_drop_flag") == 1, "BATTERY_DROP"),
            when(col("fault_flag_final") == 1, "FAULT")
        )).alias("anomaly_type")
    ).filter(col("anomaly_type").isNotNull()) \
     .groupBy("deviceID", "anomaly_type") \
     .count()

    save_fn(df_anomaly_summary, base_path, ["aggregated", "anomaly_summary"], None)

    log.info("Anomaly summary created")

    # ---------------- DATA QUALITY CHECKS ---------------- #

    # Null count
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    log.info("Null counts calculated")

    # Duplicate count
    duplicate_count = df.count() - df.dropDuplicates().count()
    log.info(f"Duplicate records: {duplicate_count}")

    # Drift detection (daily avg temp)
    df_drift = df.groupBy("year", "month", "day").agg(
        avg("cTemp").alias("daily_avg_temp")
    )

    log.info("Drift detection completed")

    return {
        "trip": df_trip,
        "device": df_device,
        "hour": df_hour,
        "day": df_day,
        "anomaly_summary": df_anomaly_summary
    }