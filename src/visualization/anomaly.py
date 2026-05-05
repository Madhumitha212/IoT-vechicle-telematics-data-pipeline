import os
import matplotlib.pyplot as plt

def generate_anomaly_plots(spark, base_path, plot_dir, log):
    
    os.makedirs(plot_dir, exist_ok=True)
    
    log.info("Reading anomaly dataset")
    df = spark.read.parquet(f"{base_path}/processed/anomaly/")
    pdf = df.limit(500).toPandas()
    anomalies = pdf[pdf['is_anomaly'] == 1]

    # Speed anomalies
    log.info("Plotting speed anomalies")
    plt.figure(figsize=(12,6))
    plt.plot(pdf['timeStamp'], pdf['gps_speed'], label="Speed", color="blue")
    plt.scatter(anomalies['timeStamp'], anomalies['gps_speed'], color="red", marker="x", label="Anomalies")
    plt.title("Speed Over Time with Anomalies")
    plt.xlabel("Time")
    plt.ylabel("Speed")
    plt.legend()
    plt.savefig(os.path.join(plot_dir, "speed_anomalies.png"))
    plt.close()

    # Temperature anomalies
    log.info("Plotting temperature anomalies")
    plt.figure(figsize=(12,6))
    plt.plot(pdf['timeStamp'], pdf['cTemp'], label="Temperature", color="orange")
    plt.scatter(anomalies['timeStamp'], anomalies['cTemp'], color="red", marker="o", label="Anomalies")
    plt.title("Temperature Over Time with Anomalies")
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.legend()
    plt.savefig(os.path.join(plot_dir, "temp_anomalies.png"))
    plt.close()
