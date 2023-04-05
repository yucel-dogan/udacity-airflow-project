from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    #The DAG does not have dependencies on past runs
    'depends_on_past': False,
    #On failure, the task are retried 3 times
    'retries': 3,
    #Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    #Catchup is turned off
    'catchup': False,
    #Do not email on retry
    'email_on_retry': False
}

dag = DAG('final_project_legacy',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )


start_operator = DummyOperator(task_id='start_execution_task' ,  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    file_format="json",
    json_path="s3://udacity-dend/log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/",    
    file_format="json",
    json_path='auto',
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    mode="delete-load"
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    mode="delete-load"
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    mode="delete-load"
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    mode="delete-load"
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "songplays", "songs", "artists",  "time", "users"]
)

end_operator = DummyOperator(task_id='end_execution_task',  dag=dag)

#1st level
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

#2nd level
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

#3rd level
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

#4th level
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

#data quality controls
run_quality_checks >> end_operator