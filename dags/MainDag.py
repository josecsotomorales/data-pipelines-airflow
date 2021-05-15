from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Default args per project specifications
default_args = {
    'owner': 'josecsotomorales',
    'start_date': datetime(2021, 5, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('sparkify_data_pipelines',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    provide_context=False,
    dag=dag,
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    provide_context=False,
    dag=dag,
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    region="us-west-2",
    data_format="JSON",
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplay",
    sql="songplay_table_insert",
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql="user_table_insert",
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="song",
    sql="song_table_insert",
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artist",
    sql="artist_table_insert",
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplay", "users", "song", "artist", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator