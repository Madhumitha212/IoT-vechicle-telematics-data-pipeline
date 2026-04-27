import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv
from config.logger import logger

# ---------------- ENV ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(BASE_DIR, "config", ".env")

load_dotenv(env_path)
URL = os.getenv("API_URL")

# ---------------- DATA ----------------
df = pd.read_csv("datasets/v2.csv")
logger.info(f"Dataset loaded: {df.shape}")

batch_size = 100
batch_id = 1   #  add batch tracking

# ---------------- INGESTION LOOP ----------------
for i in range(0, len(df), batch_size):

    batch = df.iloc[i:i + batch_size].to_dict(orient="records")

    start_row = i
    end_row = min(i + batch_size, len(df))

    log = logger.bind(
        batch_id=batch_id,
        start_row=start_row,
        end_row=end_row,
        batch_size=len(batch)
    )

    log.info("Processing batch started")

    try:
        response = requests.post(URL, json=batch, timeout=10)

        if response.status_code == 200:
            log.info("Batch SUCCESS")
        else:
            log.error(f"Batch FAILED | response={response.text}")

    except Exception as e:
        log.exception(f"Batch ERROR: {e}")

    time.sleep(3)
    batch_id += 1

logger.info("Ingestion completed successfully")