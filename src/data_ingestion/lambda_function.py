import json
import boto3
import uuid
import logging
import os
from datetime import datetime, timezone

# S3 client
s3 = boto3.client("s3")

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config
BUCKET_NAME = os.getenv("BUCKET_NAME")

REQUIRED_FIELDS = [
    "timeStamp", "tripID", "accData", "deviceID", 
    "gps_speed", "battery", "cTemp", "dtc", "eLoad", "iat"
]
INTEGER_FIELDS = ["gps_speed", "battery", "cTemp", "eLoad", "iat", "dtc"]

def validate_record(data):
    missing_fields = [f for f in REQUIRED_FIELDS if f not in data]
    if missing_fields:
        return False, f"Missing fields: {missing_fields}", data

    if not isinstance(data["timeStamp"], str):
        return False, "timeStamp must be string", data

    type_mismatch = [
        f for f in INTEGER_FIELDS
        if not isinstance(data[f], (int, float)) and data[f] is not None
    ]
    if type_mismatch:
        return False, f"Type mismatch: {type_mismatch}", data
    
    data = {k: v for k, v in data.items() if k in REQUIRED_FIELDS}
    return True, None, data


def lambda_handler(event, context):
    try:
        print(event)
        if "body" in event:
            try:
                records = json.loads(event["body"])
            except Exception:
                return {"statusCode": 400, "body": "Invalid JSON"}
        else:
            records = event

        if not isinstance(records, list):
            return {
                "statusCode": 400,
                "body": "Expected a list of records"
            }

        valid_records = []
        failed_records = []

        # Process each record
        for idx, data in enumerate(records, start=1):
            is_valid, error, data = validate_record(data)

            if not is_valid:
                failed_records.append({"index": idx, "error": error})
                continue

            ingestion_time = datetime.now(timezone.utc)
            data["ingestion_timestamp"] = ingestion_time.isoformat()

            # Flags
            if data["cTemp"] is not None and data["cTemp"] > 90:
                data["high_temp_flag"] = True
            else:
                data["high_temp_flag"] = False

            if data["battery"] is not None and data["battery"] < 30:
                data["low_battery_flag"] = True
            else:
                data["low_battery_flag"] = False

            if data["dtc"] is not None and data["dtc"]> 0:
                data["fault_flag"] = True
            else:
                data["fault_flag"] = False

            # Add records to valid_records list
            valid_records.append(data)

        if not valid_records:
            return {
                "statusCode": 400,
                "body": {
                    "message": "All records failed",
                    "errors": failed_records
                }
            }

        # Partition 
        now = datetime.now(timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        hour = now.strftime("%H")

        file_key = (
            f"raw/year={year}/month={month}/day={day}/hour={hour}/"
            f"batch_{uuid.uuid4()}.json"
        )

        ndjson_data = "\n".join(json.dumps(record) for record in valid_records)

        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=ndjson_data,
            ContentType="application/json"
        )

        logger.info(f"Stored {len(valid_records)} records at {file_key}")

        return {
            "statusCode": 200,
            "body": {
                "message": "Batch processed successfully",
                "total": len(records),
                "valid": len(valid_records),
                "failed": len(failed_records),
                "s3_key": file_key
            }
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")

        return {
            "statusCode": 500,
            "body": {
                "error": "Internal Server Error",
                "details": str(e)
            }
        }