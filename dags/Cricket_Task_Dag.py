from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from Cricket_Task import extract_dfs, transformation, load
from Cricket_Task_Upload_HDFS import upload
from airflow.models import Variable
import time


def etl():

    DB_URI = Variable.get("db_uri")
    DB_USER = Variable.get("db_user")
    DB_PASSWORD = Variable.get("db_password")
    HDFS_URL = Variable.get("hdfs_url")

    dbinfo = {"DATABASE_URI": f"jdbc:{DB_URI}",
              "DATABASE_PROPERTIES": {"user": DB_USER, "password": DB_PASSWORD}}

    start_time = time.time()

    file = open("report.txt", "a")

    (player,
        team,
        match,
        player_match,
        ball_by_ball
     ) = extract_dfs(hdfs_url=HDFS_URL)

    extraction_time = time.time() - start_time
    print("--- %s seconds ---" % (extraction_time))
    file.write(f'Extraction Time : {extraction_time} s \n')
    extraction_time = time.time()

    (most_winning_team,
        season_batsman_stats,
        season_batsman_percentage_growth,
        season_bowler_stats,
        season_bowler_economy_stats,
        player_teams_count,
        batsman_strike_rate,
        bowler_economy_rate,
        winning_chance_for_toss_win,
        home_away_stats) = transformation(player, team, match, player_match, ball_by_ball)

    transformation_time = time.time() - extraction_time
    print("--- %s seconds ---" % (transformation_time))
    file.write(f'Transformation Time : {transformation_time} s \n')
    transformation_time = time.time()

    load(most_winning_team,
         season_batsman_stats,
         season_batsman_percentage_growth,
         season_bowler_stats,
         season_bowler_economy_stats,
         player_teams_count,
         batsman_strike_rate,
         bowler_economy_rate,
         winning_chance_for_toss_win,
         home_away_stats,
         dbinfo)

    load_time = time.time() - transformation_time
    print("--- %s seconds ---" % (load_time))
    file.write(f'Load Time : {load_time} s \n')

    full_time = time.time()-start_time
    print("--- %s seconds ---" % (full_time))
    file.write(f'Full Time : {full_time} s \n \n')


def upload_to_hdfs():
    datapath = Variable.get('datapath')
    upload(datapath)


with DAG('bigdata-cricket-task-dag',
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=False
         ) as dag:

    run_hdfs = BashOperator(
        task_id='run_hdfs_cricket_task',
        bash_command="$HADOOP_HOME/sbin/start-dfs.sh "
    )

    upload_data = PythonOperator(
        task_id='upload_data_cricket_task',
        python_callable=upload_to_hdfs
    )

    etl_task = PythonOperator(
        task_id='etl_bigdata_cricket_task',
        python_callable=etl
    )

    stop_hdfs = BashOperator(
        task_id='stop_hdfs_cricket_task',
        bash_command="$HADOOP_HOME/sbin/stop-dfs.sh "
    )

    run_hdfs >> upload_data >> etl_task >> stop_hdfs
