import os
import matplotlib.pyplot as plt
import seaborn as sns

def generate_trip_analysis_plots(spark, base_path, plot_dir, log):
    os.makedirs(plot_dir, exist_ok=True)

    log.info("Reading raw trip data")
    # Speed distribution (raw data)
    df_trip_raw = spark.read.parquet(f"{base_path}/processed/cleaned/")
    pdf_trip_raw = df_trip_raw.limit(500).toPandas()
    log.info("Plotting speed distribution per trip")
    plt.figure(figsize=(10,6))
    sns.boxplot(x="tripID", y="gps_speed", data=pdf_trip_raw)
    plt.title("Distribution of Speed per Trip")
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(plot_dir, "speed_distribution_per_trip.png"))
    plt.close()

    
    # Trip duration (aggregated data)
    log.info("Reading aggregated trip data")
    df_trip = spark.read.parquet(f"{base_path}/aggregated/trip/")
    pdf_trip = df_trip.limit(500).toPandas()
    log.info("Plotting trip duration comparison")
    plt.figure(figsize=(10,6))
    plt.bar(pdf_trip['tripID'], pdf_trip['trip_duration'])
    plt.title("Trip Duration Comparison")
    plt.xticks(rotation=45)
    plt.ylabel("Duration (seconds)")
    plt.savefig(os.path.join(plot_dir, "trip_duration_comparison.png"))
    plt.close()
