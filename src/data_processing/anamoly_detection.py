from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.logger import logger

# ------------------ THRESHOLDS ------------------ #
TEMP_THRESHOLD = 90
LOAD_THRESHOLD = 80
SPEED_SPIKE_THRESHOLD = 30
BATTERY_DROP_THRESHOLD = 0.5
Z_SCORE_THRESHOLD = 2


def detect_anomalies(df, save_fn, base_path):

    log = logger.bind(stage="anomaly")
    log.info("Starting anomaly detection")

    df = df.repartition("deviceID")

    # ---------------- RULE-BASED FLAGS ---------------- #
    df = df.withColumns({
        "overheat_flag": when(col("cTemp") > TEMP_THRESHOLD, 1).otherwise(0),
        "high_load_flag": when(col("eLoad") > LOAD_THRESHOLD, 1).otherwise(0),
        "speed_spike_flag": when(col("speed_change") > SPEED_SPIKE_THRESHOLD, 1).otherwise(0),
        "battery_drop_flag": when(col("battery_drain_rate") > BATTERY_DROP_THRESHOLD, 1).otherwise(0),
        "fault_flag_final": when(col("dtc") != 0, 1).otherwise(0)
    })

    log.info("Rule-based flags added")

    # ---------------- STATISTICAL ANOMALY ---------------- #
    device_window = Window.partitionBy("deviceID")

    df = df.withColumns({
        "mean_cTemp": avg("cTemp").over(device_window),
        "std_cTemp": stddev("cTemp").over(device_window)
    })

    df = df.withColumn(
        "z_score_temp",
        when(col("std_cTemp") != 0,
             (col("cTemp") - col("mean_cTemp")) / col("std_cTemp"))
        .otherwise(0)
    )

    df = df.withColumn(
        "stat_anomaly_flag",
        when(abs(col("z_score_temp")) > Z_SCORE_THRESHOLD, 1).otherwise(0)
    )

    log.info("Statistical anomaly detection completed")

    # ---------------- FINAL FLAG ---------------- #
    df = df.withColumn(
        "is_anomaly",
        (
            col("overheat_flag") +
            col("high_load_flag") +
            col("speed_spike_flag") +
            col("battery_drop_flag") +
            col("fault_flag_final") +
            col("stat_anomaly_flag")
        ) > 0
    )

    log.info("Final anomaly flag created")

    # ---------------- SELECT REQUIRED COLUMNS ---------------- #
    columns_to_keep = [
        "deviceID", "tripID", "timeStamp",
        "gps_speed", "cTemp", "battery",
        "year", "month", "day", "hour",
        "overheat_flag", "high_load_flag", "speed_spike_flag",
        "battery_drop_flag", "fault_flag_final", "z_score_temp", "stat_anomaly_flag",
        "is_anomaly"
    ]

    df = df.select(*columns_to_keep)

    log.info("Selected required columns")

    # ---------------- SAVE (MANDATORY) ---------------- #
    log.info("Saving anomaly dataset")

    save_fn(
        df,
        base_path,
        folders=["processed", "anomaly"],
        partition_cols=["year", "month", "day"]
    )

    log.info("Anomaly dataset saved successfully")

    return df