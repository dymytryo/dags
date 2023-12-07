from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys
import psycopg2


def _create_logger(name: str = "custom_logger", level: int = logging.DEBUG) -> logging.Logger:
    """Return a Python logger that prints to stdout with default formatting.

    Args:
        name (str, optional): Name of the logger. Useful for filtering logs by source.
        Defaults to "custom_logger".

        level (int, optional): The logging level that logs should be emitted at.
        Defaults to logging.DEBUG.

    Returns:
        logging.Logger: Logger with sane defaults enabled.
    """
    # Grab the global logger for the provided name, and set its default level as needed.
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # If any other handlers were added previously, remove them.
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    # Specify log and date format for stdout.
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | [%(filename)s:%(lineno)d] | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    return logger


def drop_redshift_table(**kwargs) -> None:
    """
    Drop multiple Amazon Redshift tables using psycopg2.

    Parameters:
    - table_names: A list of Amazon Redshift table names.
    """
    logger = _create_logger('drop_redshift_table_logger', logging.DEBUG)

    logger.info(f"Fetching table names from config")
    table_names = kwargs.get('dag_run').conf['table_names']
    logger.info(f"Fetched table names from config")

    for table_name in table_names:
        try:
            with psycopg2.connect(host=Variable.get('REDSHIFT_HOST'),
                                  database=Variable.get('REDSHIFT_DBNAME'),
                                  user=Variable.get('REDSHIFT_USER'),
                                  password=Variable.get('REDSHIFT_PASSWORD'),
                                  port=5439) as conn:
                with conn.cursor() as cur:
                    logger.info(f"Checking table type {table_name}")
                    cur.execute(f"SELECT table_type FROM information_schema.tables WHERE table_schema || '.' || table_name = '{table_name}';")
                    result = cur.fetchone()

                    if result:
                        if result[0] == 'BASE TABLE':
                            logger.info(f"Dropping table {table_name}")
                            cur.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE;')
                            logger.info(f"Table {table_name} dropped successfully.")
                        elif result[0] == 'VIEW':
                            logger.info(f"Dropping view {table_name}")
                            cur.execute(f'DROP VIEW IF EXISTS {table_name} CASCADE;')
                            logger.info(f"View {table_name} dropped successfully.")
                    else:
                        logger.warning(f"{table_name} not found in the database.")

        except Exception as e:
            logger.exception(f"Error while dropping table {table_name}: {e}")
            raise e


# Define default args for pacing DAG
default_args = {
    'owner': 'dymytryo',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'email': ["dymytryo"],
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

with DAG(
        dag_id='dbt_drop_redshift_view_or_table',
        default_args=default_args,
        description='A simple DAG to drop a table(for instance, incremental models)',
        schedule_interval=None,
        start_date=datetime(2023, 8, 11),
        catchup=False
) as dag:

    dag.doc_md = """
        ```
        {
        "table_names": ["your_table_schema_here.your_first_table_name_here", "your_table_schema_here.your_second_table_name_here", ...]
        }
        ```
    """

    drop_redshift_table_task = PythonOperator(
        task_id='drop_redshift_table',
        python_callable=drop_redshift_table,
        sla=timedelta(minutes=15)
    )
