import os
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from astro import sql as aql
from astro.table import Table
import pandas as pd

parent_folder = '/opt/rh/sql_scripts'
files = sorted(os.listdir(parent_folder))


@aql.dataframe()
def show_data(df: pd.DataFrame):
    print(df)


@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False, max_active_runs=1)
def partners_dag():
    for file_name in files:
        load_data = aql.transform_file(file_path=f'{parent_folder}/{file_name}', conn_id="sqlite_default",
                                       parameters={"input_table": os.path.splitext(file_name)[0]})
        show_data(load_data)


dag = partners_dag()
