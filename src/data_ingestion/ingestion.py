import requests
import csv
import time
import os
from dotenv import load_dotenv
from config.logger import logger

load_dotenv()

API_URL = os.getenv("API_URL")

file_path = "datasets/v2.csv"

BATCH_SIZE = 200
batch = []

with open(file_path, mode='r') as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader, start=1):
        try:
            data = {
                "timeStamp": row['timeStamp'],
                "tripID": int(float(row['tripID'])) if row['tripID'] else None,
                "accData": row['accData'] if row['accData'] else None,
                "gps_speed": float(row['gps_speed']) if row['gps_speed'] else None,
                "battery": float(row['battery']) if row['battery'] else None,
                "cTemp": float(row['cTemp']) if row['cTemp'] else None,
                "dtc": int(float(row['dtc'])) if row['dtc'] else None,
                "iat": float(row['iat']) if row['iat'] else None,
                "eLoad": float(row['eLoad']) if row['eLoad'] else None,
                "deviceID": int(float(row['deviceID'])) if row['deviceID'] else None   
            }
            batch.append(data)

            # When batch size reaches 200 send API request
            if len(batch) == BATCH_SIZE:
                response = requests.post(API_URL, json=batch, timeout=200)
                print(response)
                try:
                    resp_json = response.json()
                    print(resp_json)
                except Exception:
                    resp_json = {"message": response.text}

                if response.status_code == 200:
                    logger.bind(
                        start_row=i-BATCH_SIZE+1,
                        end_row=i,
                        s3_key=resp_json['body'].get("s3_key"),
                        valid_records=resp_json['body'].get("valid"),
                        failed_records=resp_json['body'].get("failed")
                    ).info(f"Batch uploaded successfully")
                else:
                    logger.bind(
                        start_row=i-BATCH_SIZE+1,
                        end_row=i,
                        response=resp_json
                    ).error(f"Batch failed")
            
                
                batch = []  
                time.sleep(30)  # dealy between batches
                logger.info("Going to next request")
                
        except Exception as e:
            logger.error(f"Row {i} error: {str(e)}")

    # Send remaining records
    if batch:
        response = requests.post(API_URL, json=batch, timeout=2000)
        try:
            resp_json = response.json()
        except Exception:
            resp_json = {"message": response.text}

        if response.status_code == 200:
            logger.bind(
                start_row=i-len(batch)+1,
                end_row=i,
                s3_key=resp_json.get("s3_key"),
                valid_records=resp_json.get("valid"),
                failed_records=resp_json.get("failed")
            ).info("Final batch uploaded successfully")
        else:
            logger.bind(
                start_row=i-len(batch)+1,
                end_row=i,
                response=resp_json
            ).error("Final batch failed")

logger.info("Streaming completed successfully")