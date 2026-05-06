from pyspark.sql.window import Window
from pyspark.sql.functions import *
from config.logger import logger


def add_window_features(df, save_fn , base_path):

    log = logger.bind(stage="window")
    log.info("Starting window-based computations")

    # --- Repartition by deviceID with fixed number of partitions ---
    df = df.repartition(200, "deviceID")

    # ---------------- WINDOW DEFINITIONS ---------------- #
    base_window = Window.partitionBy("deviceID").orderBy("timeStamp")
    rolling_window = base_window.rowsBetween(-5, 0)

    # ---------------- ROLLING METRICS ---------------- #
    df = df.withColumns({
        "rolling_avg_speed": round(avg("gps_speed").over(rolling_window), 2),
        "rolling_avg_temp": round(avg("cTemp").over(rolling_window), 2),
        "rolling_std_temp": round(stddev("cTemp").over(rolling_window), 2)
    })
    log.info("Rolling metrics added")

    # ---------------- LAG FEATURES ---------------- #
    df = df.withColumns({
        "previous_speed": lag("gps_speed").over(base_window),
        "previous_temp": lag("cTemp").over(base_window),
        "previous_battery": lag("battery").over(base_window),
        "previous_time": lag("timeStamp").over(base_window)
    })
    log.info("Lag features added")

    # ---------------- RATE CALCULATIONS ---------------- #
    df = df.withColumns({
        "speed_change": col("gps_speed") - col("previous_speed"),
        "temperature_change": col("cTemp") - col("previous_temp"),
        "time_diff": unix_timestamp("timeStamp") - unix_timestamp("previous_time"),
        "battery_drop": col("previous_battery") - col("battery")
    })

    df = df.withColumn(
        "battery_drain_rate",
        when(col("time_diff") > 0,
             col("battery_drop") / col("time_diff"))
        .otherwise(0)
    )
    log.info("Rate calculations completed")

    # ---------------- FINAL COLUMN SELECTION ---------------- #
    columns_to_keep = [
        "deviceID","tripID","timeStamp",
        "gps_speed","cTemp","battery",
        "year","month","day","hour",
        "rolling_avg_speed","rolling_avg_temp",
        "speed_change","temperature_change","battery_drain_rate",
        "vehicle_status"
    ]
    df = df.select(*columns_to_keep)
    log.info("Column selection completed")

    # ---------------- SAVE (MANDATORY) ---------------- #
    log.info("Saving window-based dataset")
    save_fn(
        df,
        base_path,
        folders=["processed", "time_series"],
        partition_cols=["year", "month", "day", "hour"]
    )
    log.info("Window-based dataset saved")

    return df
