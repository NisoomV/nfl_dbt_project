from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import nfl_data_py as nfl
import snowflake.connector
import pandas as pd

def upload_pbp_data_to_snowflake():
    try:
        games = nfl.import_pbp_data([2024])
        games = games.where(pd.notnull(games), None)

        conn = snowflake.connector.connect(
            user='username',
            password='password',
            account='account',
            warehouse='nfl_pbp',
            database='nfl_raw',
            schema='play_by_play',
            role='nfl_role'
        )

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT game_id FROM PLAY_BY_PLAY.PBP_RAW")
        existing_game_ids = set(row[0] for row in cur.fetchall())
        new_games = games[~games['game_id'].isin(existing_game_ids)]

        numberOfColumns = len(games.columns)
        placeholder = ", ".join(["%s"] * numberOfColumns)

        insert_query = f"""
            INSERT INTO PLAY_BY_PLAY.PBP_RAW
            VALUES ({placeholder})
        """
        
        for row in new_games.itertuples(index=False):
            # Convert row to tuple and replace NaNs with None explicitly
            row_values = tuple(None if pd.isna(value) else value for value in row)
            cur.execute(insert_query, row_values)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {str(e)}")
        raise



with DAG(
    dag_id="upload_pbp_to_snowflake_database",
    default_args={
        "depends_on_past": False,
        "email": ["vashinav@sheridancollege.ca"],
        "start_date": datetime(2024,10,14),
        "email_on_failure": False,
        "email_on retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10)
    },
    description="Imports data from nfl_data_py and uploads to snowflake database",
    schedule=timedelta(days=4)
) as dag:

    upload_task = PythonOperator(
        task_id='upload_pbp_data',
        python_callable=upload_pbp_data_to_snowflake
    )

    upload_task
