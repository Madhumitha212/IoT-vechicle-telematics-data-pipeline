import matplotlib.pyplot as plt
import os
import seaborn as sns

def generate_aggregation_plots(spark, base_path, plot_dir, log):
    os.makedirs(plot_dir, exist_ok=True)

    log.info("Reading device aggregation data")
    # Device aggregation
    df_device = spark.read.parquet(f"{base_path}/aggregated/device/")
    pdf_device = df_device.limit(500).toPandas()
    
    log.info("Plotting average speed per device")
    plt.figure(figsize=(10,6))
    plt.bar(pdf_device["deviceID"], pdf_device["avg_speed"])
    plt.title("Average Speed per Device")
    plt.xlabel("Device ID")                    
    plt.ylabel("Average Speed") 
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(plot_dir, "device_avg_speed.png"))
    plt.close()

    # Hourly trends
    log.info("Reading hourly aggregation data")
    df_hour = spark.read.parquet(f"{base_path}/aggregated/time_hour/").toPandas()
    plt.figure(figsize=(12,6))
    sns.lineplot(x="hour", y="avg_speed", data=df_hour)
    plt.title("Hourly Speed Trends")
    plt.xlabel("Hour of Day")                  
    plt.ylabel("Average Speed") 
    plt.savefig(os.path.join(plot_dir, "hourly_trends.png"))
    plt.close()

    # Heatmap: device vs time
    log.info("Plotting hourly trends")
    pivot = df_hour.pivot_table(index="deviceID", columns="hour", values="avg_speed")
    plt.figure(figsize=(12,8))
    sns.heatmap(pivot, cmap="coolwarm")
    plt.title("Device vs Hourly Speed Heatmap")
    plt.xlabel("Hour of Day")                  
    plt.ylabel("Device ID")  
    plt.savefig(os.path.join(plot_dir, "device_hour_heatmap.png"))
    plt.close()
