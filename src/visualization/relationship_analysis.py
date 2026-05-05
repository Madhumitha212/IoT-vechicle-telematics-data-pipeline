import os
import matplotlib.pyplot as plt

def generate_relationship_plots(spark, base_path, plot_dir, log):
    os.makedirs(plot_dir, exist_ok=True)

    log.info("Reading cleaned data for relationship analysis")
    df_rel = spark.read.parquet(f"{base_path}/processed/cleaned/")
    pdf_rel = df_rel.limit(500).toPandas()

    # Speed vs Load
    log.info("Plotting speed vs engine load")
    plt.figure(figsize=(8,6))
    plt.scatter(pdf_rel['gps_speed'], pdf_rel['eLoad'], alpha=0.5, color="blue")
    plt.title("Speed vs Engine Load")
    plt.xlabel("Speed")
    plt.ylabel("Engine Load")
    plt.savefig(os.path.join(plot_dir, "speed_vs_load.png"))
    plt.close()

    # Temperature vs Load
    log.info("Plotting temperature vs engine load")
    plt.figure(figsize=(8,6))
    plt.scatter(pdf_rel['cTemp'], pdf_rel['eLoad'], alpha=0.5, color="orange")
    plt.title("Temperature vs Engine Load")
    plt.xlabel("Temperature (°C)")
    plt.ylabel("Engine Load")
    plt.savefig(os.path.join(plot_dir, "temp_vs_load.png"))
    plt.close()
