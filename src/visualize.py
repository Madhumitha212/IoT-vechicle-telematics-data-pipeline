from src.process import create_session
from config.logger import logger
import os

# Import visualization functions
from src.visualization.time_series import generate_time_series_plots
from src.visualization.anomaly import generate_anomaly_plots
from src.visualization.trip_analysis import generate_trip_analysis_plots
from src.visualization.relationship_analysis import generate_relationship_plots
from src.visualization.aggregation_charts import generate_aggregation_plots

def main():
    log = logger.bind(stage="visualization")
    log.info("Starting visualization")

    # Create Spark session
    spark = create_session("Visualization")
    base_path = "s3a://vehicle-telematics-bucket"

    # ===============================
    # CREATE DIRECTORIES
    # ===============================
    base_dir = os.path.dirname(__file__)   # src/

    # Go inside existing visualization folder
    visualization_dir = os.path.join(base_dir, "visualization")

    # Final plots folder
    plot_dir = os.path.join(visualization_dir, "plots")

    # Create only plots (visualization already exists)
    os.makedirs(plot_dir, exist_ok=True)

    log.info(f"Plots will be saved at: {plot_dir}")

    try:
        # ===============================
        # CALL VISUALIZATION FUNCTIONS
        # ===============================
        log.info("Starting time series visualizations")
        generate_time_series_plots(spark, base_path, os.path.join(plot_dir, "time_series"), log)

        log.info("Starting anomaly visualizations")
        generate_anomaly_plots(spark, base_path, os.path.join(plot_dir, "anomalies"), log)

        log.info("Starting trip analysis visualizations")
        generate_trip_analysis_plots(spark, base_path, os.path.join(plot_dir, "trip_analysis"), log)

        log.info("Starting relationship analysis visualizations")
        generate_relationship_plots(spark, base_path, os.path.join(plot_dir, "relationships"), log)
        
        log.info("Starting aggregation visualizations")
        generate_aggregation_plots(spark, base_path, os.path.join(plot_dir, "aggregations"), log)

        log.info("All visualizations generated successfully!")

    except Exception as e:
        log.error(f"Visualization failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
