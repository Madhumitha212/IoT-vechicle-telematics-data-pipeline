from pyspark.sql.functions import *
from config.logger import logger


def clean_data(df, save_fn, base_path):

    log = logger.bind(stage="cleaning")
    log.info("Starting data cleaning")

    # ---------------- DROP OLD PARTITION COLS ---------------- #
    df = df.drop("year", "month", "day", "hour")

    # ---------------- NULL HANDLING ---------------- #
    df = df.dropna(subset=["deviceID", "timeStamp"])
    log.info("Removed null values")

    # ---------------- REMOVE DUPLICATES ---------------- #
    df = df.dropDuplicates(["tripID", "deviceID", "timeStamp"])
    log.info("Removed duplicates")

    # ---------------- FILTER INVALID VALUES ---------------- #
    df = df.filter(
        (col('gps_speed') >= 0) &
        (col('battery').between(0, 100)) &
        (col('cTemp').between(-40, 150))
    )
    log.info("Filtered invalid values")

    # ---------------- TIMESTAMP CONVERSION ---------------- #
    df = df.withColumn(
        "timeStamp",
        to_timestamp(col("timeStamp"), "yyyy-MM-dd HH:mm:ss")
    )
    log.info("Converted timestamp")

    # ---------------- TYPE CASTING ---------------- #
    df = df.withColumns({
        "gps_speed": col("gps_speed").cast("double"),
        "battery": col("battery").cast("double"),
        "cTemp": col("cTemp").cast("double"),
        "eLoad": col("eLoad").cast("double"),
        "iat": col("iat").cast("double")
    })
    log.info("Casted numeric columns")

    # ---------------- TIME FEATURES ---------------- #
    df = df.withColumns({
        "year": year(col("timeStamp")),
        "month": month(col("timeStamp")),
        "day": dayofmonth(col("timeStamp")),
        "hour": hour(col("timeStamp"))
    })
    log.info("Extracted time features")

    # ---------------- VEHICLE STATUS ---------------- #
    df = df.withColumn(
        "vehicle_status",
        when(col("cTemp") > 100, "HIGH_TEMP")
        .when(col("battery") < 20, "LOW_BATTERY")
        .otherwise("NORMAL")
    )

    df = df.repartition("year", "month", "day")

    log.info("Processed is started to save in s3")
    save_fn(
        df,
        base_path,
        folders=["processed", "cleaned"],
        partition_cols=["year", "month", "day", "hour"]
    )
    log.info("Processed data successfully saved to s3")

    log.info("Cleaned data saved successfully")

    return df