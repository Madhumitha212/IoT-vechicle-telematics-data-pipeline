# IoT Vehicle Telematics Data Pipeline

## Project Overview

This project implements a **production-grade ETL data pipeline** for processing IoT-based vehicle telemetry data. Modern vehicles generate continuous sensor data such as speed, temperature, battery level, and engine load. This pipeline ingests, processes, analyzes, and visualizes this data to derive meaningful insights and detect anomalies.

---

## System Architecture

```
Sensor/Data Source → API Gateway → Lambda → S3 (Raw)
                                        ↓
                                 PySpark ETL
                                        ↓
                       S3 (Processed + Aggregated)
                                        ↓
                            Amazon Redshift
                                        ↓
                                Visualization
```

---

## Tech Stack

- **Cloud Platform:** AWS (Lambda, S3, Redshift, API Gateway)
- **ETL Engine:** PySpark
- **Programming Language:** Python
- **Data Storage:** S3 (JSON & Parquet)
- **Data Warehouse:** Amazon Redshift
- **Visualization:** Matplotlib / Seaborn

---

## Dataset

- Source: Kaggle Vehicle Telematics Dataset
- Link : Link :https://www.kaggle.com/datasets/yunlevin/levin-vehicle-telematics/dataselect=v2.csv
- Columns:
  - tripID
  - deviceID
  - timeStamp
  - accData
  - gps_speed
  - battery
  - cTemp
  - dtc
  - eLoad
  - Iat

---

## Features Implemented

### 1. Data Ingestion (AWS Lambda)

- Real-time/batch data simulation
- Data validation (schema & types)
- Metadata enrichment:
  - ingestion_timestamp

- Rule-based flags:
  - high_temp_flag
  - low_battery_flag
  - fault_flag

- Stored in S3 (Raw Layer) in JSON format

---

### 2. Data Lake Design (S3)

- Organized structure:

  ```
  /raw/
  /processed/
  /aggregated/
  ```

- Partitioning by date/hour
- Raw data remains immutable
- Processed data stored in Parquet

---

### 3. ETL Transformation (PySpark)

#### Data Cleaning

- Removed null values
- Removed duplicates
- Filtered invalid values

#### Feature Engineering

- Extracted:
  - hour, day, week, month

#### Window-Based Computations

- Rolling metrics:
  - rolling_avg_speed
  - rolling_avg_temp
  - rolling_std_temp

- Lag features:
  - previous_speed
  - previous_temperature

- Rate calculations:
  - speed_change
  - temperature_change
  - battery_drain_rate

#### Anomaly Detection

- Engine overheating
- High engine load
- Speed spikes
- Battery drop detection
- Fault detection (dtc)
- Statistical anomaly detection (Z-score)

#### Data Enrichment

- Vehicle categorization:
  - HIGH_TEMP
  - LOW_BATTERY
  - NORMAL

- Trip duration calculation

---

### 4. Aggregations

#### Trip-Level

- Average speed
- Maximum temperature
- Trip duration

#### Device-Level

- Total records
- Average speed
- Average battery
- Anomaly count
- Fault count

#### Time-Based

- Hourly average speed
- Daily average temperature

#### Cross Metrics

- Correlation:
  - gps_speed vs eLoad
  - cTemp vs eLoad

---

### 5. Data Quality Checks

- Null value counts
- Duplicate detection
- Data drift analysis

---

### 6. Data Storage & Loading

- Stored processed data in S3 (Parquet)
- Loaded into Amazon Redshift:
  - vehicle_metrics
  - trip_metrics
  - anomaly_summary

---

### 7. Visualization

- Time-series analysis (speed & temperature)
- Rolling metrics visualization
- Anomaly highlighting
- Trip analysis
- Scatter plots for relationships
- Aggregation charts (bar, line, heatmap)

---

````
## Project Structure

```bash
IoT_vehicle_telematics_data_pipeline/
│
├── requirements.txt
├── .gitignore
├── README.md
│
├── config/
│   ├── get_client.py
│   └── logger.py
│
├── load_to_redshift.py
├── process.py
├── visualize.py
│
└── src/
    │
    ├── data_ingestion/
    │   ├── ingestion.py
    │   └── lambda_function.py
    │
    ├── data_processing/
    │   ├── data_cleaning.py
    │   ├── time_series.py
    │   ├── analytics.py
    │   └── anamoly_detection.py
    │
    ├── redshift/
    │   ├── create_load.py
    │   └── execute_query.py
    │
    └── visualization/
        │
        ├── plots/
        │   │
        │   ├── aggregations/
        │   │   ├── device_avg_speed.png
        │   │   ├── device_hour_heatmap.png
        │   │   └── hourly_trends.png
        │   │
        │   ├── anomalies/
        |   |   ├── speed_anomalies.png
        |   |   └── temp_anomalies.png
        │   ├── relationships/
        |   |   ├── speed_vs_load.png
        |   |   └── temp_vs_load.png
        │   ├── time_series_img/
        |   |   ├── rolling_avg_speed.png
        |   |   ├── rolling_avg_temp.png
        |   |   ├── speed_vs_time.png
        |   |   └── temp_vs_time.png
        │   ├── trip_analysis/
        |   |   ├── speed_distribution_per_trip.png
        |   |   └── trip_duration_comparison.png
        ├── time_series.py
        ├── trip_analysis.py
        ├── relationship_analysis.py
        ├── anomaly.py
        └── aggregation_charts.py
````

## How to Run

### 1. Setup Environment

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run ETL Pipeline and analytics

```
python -m src.process
```

### 4. Load Data to Redshift

```
python -m src.load_to_redshift
```

### 5. Generate Visualizations

```
python -m src.visualize
```

---

## Outputs Generated

- Processed dataset
- Time-series dataset
- Anomaly dataset
- Aggregated dataset
- Visual plots in plots folder under visualization folder

---

## Key Learnings

- Building scalable ETL pipelines using PySpark
- Working with AWS services (S3, Lambda, Redshift)
- Implementing window functions and time-series analytics
- Designing data lakes and warehouse architectures
- Performing anomaly detection on IoT data

---

## Conclusion

This project demonstrates an end-to-end **IoT data engineering pipeline**, covering:

- Data ingestion
- Transformation
- Storage
- Analytics
- Visualization

It provides a scalable and production-ready approach to handling real-time vehicle telemetry data.

---

## Author

```
R Madhumitha
```
