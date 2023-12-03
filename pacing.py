from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pandas.tseries.offsets import MonthEnd, CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
import logging
import sys
import psycopg2
from typing import Dict, Union, Optional, List


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


def _read_from_redshift(query: str) -> Optional[pd.DataFrame]:
    """
    Read from Amazon Redshift using a custom query and return the results as a DataFrame.

    Parameters:
    - query: The SQL query to execute.

    Returns:
    - pd.DataFrame: DataFrame with the results of the query.
    """
    logger = _create_logger('read_from_redshift_logger', logging.DEBUG)
    logger.info("Initiating database read with query")

    try:
        # Validate query string
        if not isinstance(query, str) or len(query.strip()) == 0:
            raise ValueError("The query must be a non-empty string.")

        with psycopg2.connect(host=Variable.get('REDSHIFT_HOST'),
                              database=Variable.get('REDSHIFT_DBNAME'),
                              user=Variable.get('REDSHIFT_USER'),
                              password=Variable.get('REDSHIFT_PASSWORD'),
                              port=5439) as conn:
            df = pd.read_sql_query(query, conn)

        logger.info("Successfully fetched data for query")
        return df

    except Exception as e:
        logger.exception("Error while reading from database")
        return None

    finally:
        logger.info("Finished database read operation for query")


def _calculate_projections(df: pd.DataFrame, payment_method: str) -> Dict[str, Union[str, int]]:
    """
    Calculate the pacing for a given method.
    Adjustments are made on dbt side to account for settlement differences between Comdata and Marqeta.
    Parameters:
    df (DataFrame): A DataFrame with three fields: 'reporting_date', 'payment_method', and 'reported_volume'.
    method (str): The method to calculate the pacing for.
    Returns:
    dict: A dictionary containing the pacing calculation details.
    """

    logger = _create_logger('calculate_projections_logger', logging.DEBUG)
    try:
        # Input validation
        if not isinstance(df, pd.DataFrame):
            logger.error('df is not a Pandas DataFrame')
            raise TypeError("df must be a Pandas DataFrame")

        if not isinstance(payment_method, str):
            raise TypeError("payment_method must be a string")

        required_cols = ['reporting_date', 'payment_method', 'reported_volume']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error('DataFrame is missing columns')
            raise ValueError(f"DataFrame is missing required columns: {missing_cols}")

        # Convert the column to datetime
        df['reporting_date'] = pd.to_datetime(df['reporting_date'])

        # Get current month
        current_month: datetime = datetime.now().replace(day=1)

        # Define custom work week to include Saturday and exclude Sunday
        weekmask: str = 'Tue Wed Thu Fri Sat'

        # Instantiate the calendar with federal holidays
        calendar: USFederalHolidayCalendar = USFederalHolidayCalendar()

        # Create a custom reporting day frequency
        bizday_custom: CustomBusinessDay = CustomBusinessDay(weekmask=weekmask,
                                                             calendar=calendar)

        # Define the date range
        date_range_full: pd.DatetimeIndex = pd.date_range(start=current_month,
                                                          end=current_month + MonthEnd(1),
                                                          freq=bizday_custom
                                                          )
        date_range_elapsed: pd.DatetimeIndex = pd.date_range(start=current_month,
                                                             end=datetime.now(),
                                                             freq=bizday_custom
                                                             )

        # Define the condition for each day
        condition_total: List[bool] = [day.weekday() != 5 or not ((day + MonthEnd(1) - day).days < 2
                                                                  ) for day in date_range_full]
        condition_elapsed: List[bool] = [day.weekday() != 5 or not ((day + MonthEnd(1) - day).days < 2
                                                                    ) for day in date_range_elapsed]

        # Count the number of days meeting the condition
        bizdays_month_total: int = len(condition_total)
        bizdays_month_passed: int = len(condition_elapsed)

        # Filter df for specified method
        df_method: pd.DataFrame = df[df['payment_method'] == payment_method]

        # Calculate the volume in current month
        monthly_volume: float = df_method[(df_method['reporting_date'].dt.year == current_month.year) &
                                       (df_method['reporting_date'].dt.month == current_month.month)
                                       ]['reported_volume'].sum()

        # Calculate the average daily volume
        avg_daily_volume: float = round(monthly_volume / bizdays_month_passed) if bizdays_month_passed > 0 else 0

        # Project the total volume for the month
        projected_volume: float = round(avg_daily_volume * bizdays_month_total)

        # Create a dictionary of results
        results: Dict[str, Union[str, float]] = {
            'payment_method': payment_method,
            'projected_total_volume': projected_volume
        }

        return results

    except Exception as e:
        logger.exception("Error calculating projections")
        raise e

    finally:
        logger.info("Finished calculating projections")


def _write_to_redshift(df: pd.DataFrame, table_name: str, if_exists: Optional[str] = 'replace') -> None:
    """
    Write a Pandas DataFrame to an Amazon Redshift table using psycopg2.

    Parameters:
    - df: The DataFrame to be written.
    - table_name: The name of the Amazon Redshift table.
    - if_exists: What to do if the table already exists. Options are: 'fail', 'replace', 'append'. Default is 'replace'.
    """
    logger = _create_logger('write_to_redshift_logger', logging.DEBUG)
    # Validate that DataFrame has only two columns and named correctly
    if df.shape[1] != 2 or 'payment_method' not in df.columns or 'projected_total_volume' not in df.columns:
        logger.error("DataFrame does not meet the required structure.")
        raise ValueError("DataFrame must have exactly two columns named 'method' and 'volume'.")

    try:
        with psycopg2.connect(host=Variable.get('REDSHIFT_HOST'),
                              database=Variable.get('REDSHIFT_DBNAME'),
                              user=Variable.get('REDSHIFT_USER'),
                              password=Variable.get('REDSHIFT_PASSWORD'),
                              port=5439) as conn:
            with conn.cursor() as cur:
                if if_exists == 'replace':
                    logger.info(f"Dropping table {table_name}")
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.info(f"Creating table {table_name}")
                    cur.execute(f"CREATE TABLE {table_name} (payment_method TEXT, projected_total_volume DECIMAL)")
                    logger.info(f"Inserting data into {table_name}.")
                    insert_query = f"INSERT INTO {table_name} (payment_method, projected_total_volume) VALUES (%s, %s)"

                    for _, row in df.iterrows():
                        cur.execute(insert_query, tuple(row))

                logger.info(f"Data successfully inserted into {table_name}.")

    except Exception as e:
        logger.exception(f"Error while writing to table {table_name}: {e}")
        raise e


def calculate_pacing():
    # create a logger
    logger = _create_logger(name="calculate_pacing_logger", level=logging.DEBUG)

    # declare the query
    query = """
    
    SELECT
      ...

    """

    # function to get the data
    input_df = _read_from_redshift(query)
    # crunch using the metodology designed
    output_df = pd.DataFrame([_calculate_projections(input_df, method) for method in input_df.payment_method.unique()])
    # output to the datalake
    _write_to_redshift(output_df, 'payment.pacing')

# Define default args for pacing DAG
default_args = {
    'owner': 'dymytryo',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'email': [''],
    'retries': 2,
    'retry_delay': timedelta(minutes=55)
}

with DAG(
        dag_id='vco_run_curr_month_pacing',
        default_args=default_args,
        description='A simple pacing of volume based on the number of business days and settlements for a given month.',
        schedule_interval='0 18 * * *',
        start_date=datetime(2023, 8, 8),
        catchup=False
) as dag:

    run_pacing_script = PythonOperator(
        task_id='run_pacing_script',
        python_callable=calculate_pacing,
        sla=timedelta(minutes=15)
    )
