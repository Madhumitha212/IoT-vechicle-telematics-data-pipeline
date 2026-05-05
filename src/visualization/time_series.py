import os
import matplotlib.pyplot as plt

def generate_time_series_plots(spark, base_path, plot_dir, log):
    os.makedirs(plot_dir, exist_ok=True) 

    log.info("Reading cleaned data for time series plots")
    df = spark.read.parquet(f"{base_path}/processed/cleaned/")
    pdf = df.limit(100).toPandas()

    log.info("Reading time series aggregated data")
    df_ts = spark.read.parquet(f"{base_path}/processed/time_series/")
    pdf_ts = df_ts.limit(100).toPandas()

   
    # Speed vs Time
    log.info("Plotting speed vs time")
    plt.figure(figsize=(12,6))
    plt.plot(pdf['timeStamp'], pdf['gps_speed'], color="blue")
    plt.title("Speed Over Time")
    plt.xlabel("Time")
    plt.ylabel("Speed")
    plt.savefig(os.path.join(plot_dir, "speed_vs_time.png"))
    plt.close()

    
    # Temperature vs Time
    log.info("Plotting temperature vs time")
    plt.figure(figsize=(12,6))
    plt.plot(pdf['timeStamp'], pdf['cTemp'], color="orange")
    plt.title("Temperature Over Time")
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.savefig(os.path.join(plot_dir, "temp_vs_time.png"))
    plt.close()

    # Rolling Avg Speed
    log.info("Plotting rolling average speed")
    plt.figure(figsize=(12,6))
    plt.plot(pdf_ts['timeStamp'], pdf_ts['rolling_avg_speed'], color="green")
    plt.title("Rolling Average Speed")
    plt.xlabel("Time")
    plt.ylabel("Speed")
    plt.savefig(os.path.join(plot_dir, "rolling_avg_speed.png"))
    plt.close()

    # Rolling Avg Temperature
    log.info("Plotting rolling average temperature")
    plt.figure(figsize=(12,6))
    plt.plot(pdf_ts['timeStamp'], pdf_ts['rolling_avg_temp'], color="red")
    plt.title("Rolling Average Temperature")
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.savefig(os.path.join(plot_dir, "rolling_avg_temp.png"))
    plt.close()
