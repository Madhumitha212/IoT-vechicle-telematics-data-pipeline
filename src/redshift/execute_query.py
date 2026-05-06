import time
from config.logger import logger
from config.get_client import *


def run_redshift_query(sql, wait=True):
    log = logger.bind(stage="redshift")
    log.info(f"Executing SQL: {sql.strip().split()[0]}...")

    redshift_client = get_redshift_client()
    response = redshift_client.execute_statement(
        Database=REDSHIFT_DATABASE,
        Sql=sql,
        WorkgroupName=REDSHIFT_WORKGROUP
    )
    statement_id = response['Id']
    log.info(f"Statement submitted (ID={statement_id})")

    if not wait:
        return statement_id

    while True:
        status = redshift_client.describe_statement(Id=statement_id)
        current_status = status['Status']
        log.info(f"Query status: {current_status}")

        if current_status in ['FINISHED', 'FAILED', 'ABORTED']:
            break
        time.sleep(2)

    if current_status != 'FINISHED':
        log.error(f"Query failed: {status.get('Error', status)}")
        raise Exception(f"Query failed: {status.get('Error', status)}")

    log.success("Query executed successfully")
    return statement_id
