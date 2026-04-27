import json
import boto3
import logging
import uuid
from datetime import datetime

# ---------------- LOGGER ----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
BUCKET_NAME = "vehicle-telematics-bucket"


def lambda_handler(event, context):

    logger.info("Lambda triggered")

    try:
        body = json.loads(event["body"])
        logger.info(f"Received records: {len(body)}")

    except Exception as e:
        logger.error(f"Invalid request body: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid input')
        }

    processed = []
    failed_count = 0

    for record in body:

        try:
            if "tripID" not in record or "deviceID" not in record or "timeStamp" not in record:
                failed_count += 1
                continue

            record["gps_speed"] = float(record.get("gps_speed", 0))
            record["battery"] = float(record.get("battery", 0))
            record["cTemp"] = float(record.get("cTemp", 0))
            record["eLoad"] = float(record.get("eLoad", 0))
            record["iat"] = float(record.get("iat", 0))

            record["ingestion_timestamp"] = datetime.now().isoformat()

            record["high_temp_flag"] = 1 if record["cTemp"] > 90 else 0
            record["low_battery_flag"] = 1 if record["battery"] < 20 else 0
            record["fault_flag"] = 1 if record.get("dtc", 0) != 0 else 0

            processed.append(record)

        except Exception as e:
            failed_count += 1
            logger.error(f"Record failed: {e}")

    # ---------------- EMPTY CHECK (IMPORTANT FIX) ----------------
    if not processed:
        logger.warning("No valid records to store. Skipping S3 upload.")
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": "No valid data to store",
                "processed": 0,
                "failed": failed_count
            })
        }

    # ---------------- FILE KEY FORMAT (YOUR REQUEST) ----------------
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour

    file_key = (
        f"raw/year={year}/month={month}/day={day}/hour={hour}/"
        f"batch_{uuid.uuid4()}.json"
    )

    # ---------------- S3 UPLOAD ----------------
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(processed)
        )

        logger.info(
            f"S3 upload success | key={file_key} | "
            f"processed={len(processed)} | failed={failed_count}"
        )

    except Exception as e:
        logger.error(f"S3 upload failed: {e}")

        return {
            'statusCode': 500,
            'body': json.dumps('S3 upload failed')
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "Data stored successfully",
            "file_key": file_key,
            "processed": len(processed),
            "failed": failed_count
        })
    }