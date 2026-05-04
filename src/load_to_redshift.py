from config.logger import logger
from src.redshift.create_load import create_tables, load_data

def main():
    log = logger.bind(stage="orchestrator")
    log.info("Starting Redshift Load Phase")

    try:
        create_tables()
        load_data()
    except Exception as e:
        log.error(f"Load Phase failed: {e}")
        raise

    log.success("Redshift Load Phase completed")

if __name__ == "__main__":
    main()
