 
### Big Data Project


#### About Project:
In this project, we have taken a cricket data set from [here](https://data.world/mkhuzaima/ipl-data-till-2017).
Here we’ll try to find the answers to the following questions by analyzing the data.

1. Which team has won the most number of games?
1. Calculate the batsman strike rate in overall matches.
1. Calculate the bowler economy rate in overall matches.
1. Get statistics of home and away wins per team.
1. List teams changed by the player.
1. Calculate runs per season by batsman.
1. Obtain percentage growth of run per batsman in each season.
1. Calculate wickets taken by bowlers in each season.
1. Calculate bowler economy rate in each season.
1. What is the chance of winning the game if a team wins the toss?


#### Usage

Install all dependencies:

`$ pip install -r requirements.txt`

Check if `airflow`, `pyspark`, `hdfs` and `postgreSQL` are installed successfully or not.


Airflow Setup:
```sh
$ mkdir ~/airflow && cd ~./airflow
$ git clone git@gitlab.com:fusedataengineering/kickstart-airflow/madhav_airflow_ks.git
$ export AIRFLOW_HOME=~/airflow # or you can append it in .bashrc
$ airflow db init
# create user
$ airflow users create \
    --username <username> \
    --firstname <firstname> \
    --lastname <lastname> \
    --role Admin \
    --email <email>
$ airflow webserver --port 8080
$ airflow scheduler
```

Install `postgres` and create a database `airlflow` with some user and password

In `airflow.cfg` configure:


```py
dags_folder =<path_to_dags_directory>
sql_alchemy_conn = postgresql+psycopg2://<User>:<password>@localhost:5432/airflow
load_examples = False
```

Create a database with some `DB_NAME`

Following variables needed to be setup through airflow webui `Admin>Variables`.

| Key | Val | Description|
|--|--|--|
|datapath|<path_to_files>|Path where data folder reside eg. `/home/user/airflow/data`|
|db_uri|postgresql://localhost:5432/<DB_NAME>|Postgres Connection Info|
|db_user|<DB_USER>|Username for Database|
|db_password|\*\*\*\*\*\*| Database Password|
|hdfs_uri|hdfs://<PATH>|HDFS path|



These `variables` can also be set as:

`$ export AIRFLOW_VAR_DATAPATH=<path_to_files>`

After setting these you can open airflow in localhost:8080. You can see DAG status there.

Once the DAG is triggered, data are dumped to postgresql.

#### Visualization


```bash
cd visualization
```

Goto visualization directory and update configurations shown in `config.py` file.
```py
DB_HOST = # Postgres HOST HERE
DB_NAME = # Postgres Database Name HERE
DB_USER = # Postgres DB User HERE
DB_PASSWORD =# Postgres DB Password HERE
```

After that run `flask run` and visit `localhost:5000` to see all the answers to the questions.

