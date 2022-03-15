from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, Row, StructField, StringType, IntegerType, DateType, FloatType
from datetime import datetime

# Get jar file if you get jar error
spark = SparkSession.builder\
    .appName("Bigdata_Cricket_Task")\
    .getOrCreate()
# .config("spark.jars", "/opt/spark/jars/postgresql-42.2.22.jar") \

sc = spark.sparkContext
sc.setLogLevel('WARN')


def extract_dfs(hdfs_url):

    print('\n\n--------------------------------------------')
    print('------------- Extracting Data --------------')
    print('--------------------------------------------\n\n')

    player_schema = StructType([
        StructField('Player_Id', IntegerType(), True),
        StructField('Player_Name', StringType(), True),
        StructField('DOB', DateType(), True),
        StructField('Batting_hand', StringType(), True),
        StructField('Bowling_skill', StringType(), True),
        StructField('Country_Name', StringType(), True)
    ])
    team_schema = StructType([
        StructField('Team_Id', IntegerType(), True),
        StructField('Team_Name', StringType(), True),
    ])
    match_schema = StructType([
        StructField('match_id', IntegerType(), True),
        StructField('Team1', StringType(), True),
        StructField('Team2', StringType(), True),
        StructField('match_date', DateType(), True),
        StructField('Season_Year', IntegerType(), True),
        StructField('Venue_Name', StringType(), True),
        StructField('City_Name', StringType(), True),
        StructField('Country_Name', StringType(), True),
        StructField('Toss_Winner', StringType(), True),
        StructField('match_winner', StringType(), True),
        StructField('Toss_Name', StringType(), True),
        StructField('Win_Type', StringType(), True),
        StructField('Outcome_Type', StringType(), True),
        StructField('ManOfMach', StringType(), True),
        StructField('Win_Margin', IntegerType(), True),
        StructField('Country_id', IntegerType(), True)
    ])

    player_match_schema = StructType([
        StructField('PlayerMatch_key', IntegerType(), True),
        StructField('Match_Id', IntegerType(), True),
        StructField('Player_Id', IntegerType(), True),
        StructField('Player_Name', StringType(), True),
        StructField('DOB', DateType(), True),
        StructField('Batting_hand', StringType(), True),
        StructField('Bowling_skill', StringType(), True),
        StructField('Country_Name', StringType(), True),
        StructField('Role_Desc', StringType(), True),
        StructField('Player_team', StringType(), True),
        StructField('Opposite_Team', StringType(), True),
        StructField('Season_year', IntegerType(), True),
        StructField('is_manofThematch', IntegerType(), True),
        StructField('Age_As_on_match', IntegerType(), True),
        StructField('IsPlayers_Team_won', IntegerType(), True),
        StructField('Player_Captain', StringType(), True),
        StructField('Opposite_captain', StringType(), True),
        StructField('Player_keeper', StringType(), True),
        StructField('Opposite_keeper', StringType(), True)
    ])

    ball_by_ball_schema = StructType([
        StructField('Match_id', IntegerType(), True),
        StructField('Over_id', IntegerType(), True),
        StructField('Ball_id', IntegerType(), True),
        StructField('Innings_No', IntegerType(), True),
        StructField('Team_Batting', IntegerType(), True),
        StructField('Team_Bowling', IntegerType(), True),
        StructField('Striker_Batting_Position', IntegerType(), True),
        StructField('Extra_Type', StringType(), True),
        StructField('Runs_Scored', IntegerType(), True),
        StructField('Extra_runs', IntegerType(), True),
        StructField('Wides', IntegerType(), True),
        StructField('Legbyes', IntegerType(), True),
        StructField('Byes', IntegerType(), True),
        StructField('Noballs', IntegerType(), True),
        StructField('Penalty', IntegerType(), True),
        StructField('Bowler_Extras', IntegerType(), True),
        StructField('Out_type', StringType(), True),
        StructField('Caught', IntegerType(), True),
        StructField('Bowled', IntegerType(), True),
        StructField('Run_out', IntegerType(), True),
        StructField('LBW', IntegerType(), True),
        StructField('Retired_hurt', IntegerType(), True),
        StructField('Stumped', IntegerType(), True),
        StructField('caught_and_bowled', IntegerType(), True),
        StructField('hit_wicket', IntegerType(), True),
        StructField('ObstructingFeild', IntegerType(), True),
        StructField('Bowler_Wicket', IntegerType(), True),
        StructField('Match_Date', IntegerType(), True),
        StructField('Season', IntegerType(), True),
        StructField('Striker', IntegerType(), True),
        StructField('Non_Striker', IntegerType(), True),
        StructField('Bowler', IntegerType(), True),
        StructField('Player_Out', IntegerType(), True),
        StructField('Fielders', IntegerType(), True)
    ])

    player = spark.read.option('header', True).option(
        "dateFormat", "m/d/yyyy").schema(player_schema).csv(f'{hdfs_url}Player.csv')
    team = spark.read.option('header', True).schema(
        team_schema).csv(f'{hdfs_url}Team.csv')
    match = spark.read.option('header', True).option(
        "dateFormat", "m/d/yyyy").schema(match_schema).csv(f'{hdfs_url}Match.csv')
    player_match = spark.read.option('header', True).option(
        "dateFormat", "m/d/yyyy").schema(player_match_schema).csv(f'{hdfs_url}Player_match.csv')
    ball_by_ball = spark.read.option('header', True).schema(
        ball_by_ball_schema).csv(f'{hdfs_url}Ball_By_Ball.csv')

    print('\n\n--------------------------------------------')
    print('------------ Extraction Finished -----------')
    print('--------------------------------------------\n\n')

    return(player, team, match, player_match, ball_by_ball)


def transformation(player, team, match, player_match, ball_by_ball):

    print('\n\n--------------------------------------------')
    print('---------- Transformation Started ----------')
    print('--------------------------------------------\n\n')

    print('\n Transforming 1 :  Which team has won the most number of games? \n')
    # 1. Which team has won the most number of games?
    most_winning_team = match.groupBy('match_winner').count().orderBy(
        'count', ascending=False).first().match_winner
    most_winning_team = spark.createDataFrame(
        [Row(most_winning_team=most_winning_team)])

    print('\n Transforming 2 : Calculate runs per season by batsman. \n')
    # 2.Calculate runs per season by batsman.
    season_rows = match\
        .select('season_year')\
        .distinct()\
        .orderBy('season_year')\
        .collect()
    seasons = [season.season_year for season in season_rows]

    season_batsman_stats = {}
    for season in seasons:
        deliveries_per_season = ball_by_ball\
            .select('Striker', 'Season', 'Runs_Scored', 'Ball_id')\
            .filter(f"Season=={season}")
        season_batsman_stats[season] = deliveries_per_season\
            .withColumn('Valid_Ball', (f.col('Ball_id') < 7).cast("integer"))\
            .groupBy('Striker')\
            .agg(f.sum('Runs_Scored'), f.sum('Valid_Ball'))\
            .join(player.select('Player_Id', 'Player_Name'),
                  deliveries_per_season.Striker == player.Player_Id
                  )\
            .withColumnRenamed('sum(Runs_Scored)', 'Runs')\
            .orderBy('Runs', ascending=False)\
            .drop('Striker', 'Player_Id')\
            .withColumnRenamed('Player_Name', 'Batsman')\
            .withColumnRenamed('sum(Valid_Ball)', 'Balls')

    print('\n Transforming 3 : Obtain percentage growth of run per batsman in each season. \n')
    # 3.Obtain percentage growth of run per batsman in each season.
    season_batsman_percentage_growth = {}
    for season in seasons:
        if season != seasons[-1]:
            season_batsman_percentage_growth[f'{season}_{season+1}'] = season_batsman_stats[season].withColumnRenamed('Runs', f'Runs_{season}')                .join(
                season_batsman_stats[season +
                                     1].withColumnRenamed('Runs', f'Runs_{season+1}'),
                'Batsman',
                'inner')\
                .withColumn(f'Average_{season}_{season+1}', (f.col(f'Runs_{season+1}')+f.col(f'Runs_{season}'))/2)\
                .withColumn('Growth_Rate(%)', f.round(f.col(f'Runs_{season+1}')/f.col(f'Runs_{season}'), 2))\
                .orderBy(f'Average_{season}_{season+1}', ascending=False)\
                .drop('Balls')

    print('\n Transforming 4 : Calculate wickets taken by bowlers in each season.\n')
    # 4.Calculate wickets taken by bowlers in each season.
    season_bowler_stats = {}
    for season in seasons:
        deliveries_per_season = ball_by_ball.select(
            'Bowler', 'Season', 'Bowler_Wicket').filter(f"Season=={season}")
        season_bowler_stats[season] = deliveries_per_season\
            .groupBy('Bowler')\
            .sum('Bowler_Wicket')\
            .join(player.select('Player_Id', 'Player_Name'),
                  deliveries_per_season.Bowler == player.Player_Id
                  )\
            .withColumnRenamed('sum(Bowler_Wicket)', 'Wickets')\
            .orderBy('Wickets', ascending=False)\
            .drop('Bowler', 'Player_Id')\
            .withColumnRenamed('Player_Name', 'Bowler')

    print('\n Transforming 5 : List 3 most economical bowlers per season.\n')
    # 5.List 3 most economical bowlers per season.
    season_bowler_economy_stats = {}
    for season in seasons:
        deliveries_per_season = ball_by_ball\
            .select('Bowler', 'Season', 'Runs_Scored', 'Bowler_Extras', 'Ball_id')\
            .filter(f"Season=={season}")\
            .withColumn('Runs_Given', f.col('Runs_Scored')+f.col('Bowler_Extras'))\
            .withColumn('Valid_Ball', (f.col('Ball_id') < 7).cast("integer"))\
            .drop('Runs_Scored', 'Bowler_Extras', 'Ball_id')

        season_bowler_economy_stats[season] = deliveries_per_season\
            .groupBy('Bowler')\
            .sum('Runs_Given', 'Valid_Ball')\
            .join(player.select('Player_Id', 'Player_Name'),
                  deliveries_per_season.Bowler == player.Player_Id
                  )\
            .withColumnRenamed('sum(Runs_Given)', 'Runs_Given')\
            .drop('Bowler', 'Player_Id')\
            .withColumnRenamed('Player_Name', 'Bowler')\
            .withColumn('Overs', f.round(f.col('sum(Valid_Ball)')/6, 2))\
            .withColumn('economy', f.round(f.col('Runs_Given')/f.col('Overs'), 2))\
            .orderBy('economy', ascending=False)\
            .drop('sum(Valid_Ball)')

    print('\n Transforming 6 : List number of teams changed by the player.\n')
    # 6.List number of teams changed by the player.
    player_teams_count = player_match.groupBy('Player_Name')\
        .agg(f.collect_set('Player_team'))\
        .withColumnRenamed('collect_set(Player_team)', 'Teams')\
        .withColumn('count', f.size(f.col('Teams')))\
        .orderBy('count', ascending=False)

    print('\n Transforming 7 : List top best strike rate for the batsman according to data.\n')
    # 7. List top best strike rate for the batsman according to data.
    batsman_statistics = ball_by_ball.select('Striker', 'Season', 'Runs_Scored', 'Ball_id')\
        .withColumn('Valid_Ball', (f.col('Ball_id') < 7).cast("integer"))\
        .groupBy('Striker')\
        .agg(f.sum('Runs_Scored'), f.sum('Valid_Ball'))\
        .join(player.select('Player_Id', 'Player_Name'),
              ball_by_ball.Striker == player.Player_Id
              )\
        .withColumnRenamed('sum(Runs_Scored)', 'Runs')\
        .orderBy('Runs', ascending=False)\
        .drop('Striker', 'Player_Id')\
        .withColumnRenamed('Player_Name', 'Batsman')\
        .withColumnRenamed('sum(Valid_Ball)', 'Balls')\
        .withColumn('Strike_Rate', f.round(f.col('Runs')*100/f.col('Balls'), 2))\
        .orderBy('Runs', ascending=False)

    batsman_strike_rate = batsman_statistics.filter(
        'Balls > 100').orderBy('Strike_Rate', ascending=False)

    print('\n Transforming 8 : List the bowlers with lowest economy rate.\n')
    # 8. List the bowlers with lowest economy rate.
    bowler_statistics = ball_by_ball.select('Bowler', 'Season', 'Runs_Scored', 'Bowler_Extras', 'Ball_id')\
        .withColumn('Runs_Given', f.col('Runs_Scored')+f.col('Bowler_Extras'))\
        .withColumn('Valid_Ball', (f.col('Ball_id') < 7).cast("integer"))\
        .drop('Runs_Scored', 'Bowler_Extras', 'Ball_id')\
        .groupBy('Bowler')\
        .sum('Runs_Given', 'Valid_Ball')\
        .join(player.select('Player_Id', 'Player_Name'),
              ball_by_ball.Bowler == player.Player_Id
              )\
        .withColumnRenamed('sum(Runs_Given)', 'Runs_Given')\
        .drop('Bowler', 'Player_Id')\
        .withColumnRenamed('Player_Name', 'Bowler')\
        .withColumn('Overs', f.round(f.col('sum(Valid_Ball)')/6, 2))\
        .withColumn('economy', f.round(f.col('Runs_Given')/f.col('Overs'), 2))\
        .orderBy('Overs', ascending=False)\
        .drop('sum(Valid_Ball)')

    bowler_economy_rate = bowler_statistics.orderBy('economy', ascending=False)

    print('\n Transforming 9 : What is the chance of winning the game if a team wins the toss?\n')
    # 9. What is the chance of winning the game if a team wins the toss?
    winning_chance_for_toss_win = (match.filter(
        'Toss_Winner == match_winner').count()/match.count())*100
    winning_chance_for_toss_win = spark.createDataFrame(
        [Row(winning_chance_for_toss_win=winning_chance_for_toss_win)])

    print('\n Transforming 10 : Get statistics of home and away wins per team.\n')
    # 10. Get statistics of home and away wins per team.
    teams = [team.Team_Name for team in team.select('Team_Name').collect()]
    home_away_stats_schema = StructType([
        StructField('Team_Name', StringType(), False),
        StructField('Home_Win', FloatType(), False),
        StructField('Away_Win', FloatType(), False),
    ])

    home_away_stats = spark.createDataFrame([], schema=home_away_stats_schema)

    for team in teams:
        total_home_games = match.filter(f'Team1 == "{team}"').count()
        home_wins_count = match.filter(
            f'Team1 == "{team}" and match_winner == "{team}"').count()
        total_away_games = match.filter(f'Team2 == "{team}"').count()
        away_wins_count = match.filter(
            f'Team2 == "{team}" and match_winner == "{team}"').count()
        home_win = home_wins_count/total_home_games
        away_win = away_wins_count/total_away_games
        temp_df = spark.createDataFrame(
            [[team, home_win, away_win]], schema=home_away_stats_schema)
        home_away_stats = home_away_stats.union(temp_df)

    print('\n\n--------------------------------------------')
    print('---------- Transformation Finished ---------')
    print('--------------------------------------------\n\n')

    return(
        most_winning_team,
        season_batsman_stats, 
        season_batsman_percentage_growth, 
        season_bowler_stats, 
        season_bowler_economy_stats, 
        player_teams_count,
        batsman_strike_rate,
        bowler_economy_rate,
        winning_chance_for_toss_win,
        home_away_stats
    )


def load(
    most_winning_team,
    season_batsman_stats,
    season_batsman_percentage_growth,
    season_bowler_stats,
    season_bowler_economy_stats,
    player_teams_count,
    batsman_strike_rate,
    bowler_economy_rate,
    winning_chance_for_toss_win,
    home_away_stats,
    dbinfo
):

    print('\n\n--------------------------------------------')
    print('-------------- Writing Data ----------------')
    print('--------------------------------------------\n\n')

    most_winning_team.write.jdbc(dbinfo["DATABASE_URI"], "most_winning_team",
                                 mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    for key in season_batsman_stats:
        season_batsman_stats[key].write.jdbc(
            dbinfo["DATABASE_URI"], f"season_batsman_stats_{key}", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    for key in season_batsman_percentage_growth:
        season_batsman_percentage_growth[key].write.jdbc(
            dbinfo["DATABASE_URI"], f"season_batsman_percentage_growth_{key}", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    for key in season_bowler_stats:
        season_bowler_stats[key].write.jdbc(
            dbinfo["DATABASE_URI"], f"season_bowler_stats_{key}", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    for key in season_bowler_economy_stats:
        season_bowler_economy_stats[key].write.jdbc(
            dbinfo["DATABASE_URI"], f"season_bowler_economy_stats_{key}", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    player_teams_count.write.jdbc(dbinfo["DATABASE_URI"], "player_teams_count",
                                  mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])
    batsman_strike_rate.write.jdbc(
        dbinfo["DATABASE_URI"], "batsman_strike_rate", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])
    bowler_economy_rate.write.jdbc(
        dbinfo["DATABASE_URI"], "bowler_economy_rate", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])
    winning_chance_for_toss_win.write.jdbc(
        dbinfo["DATABASE_URI"], "winning_chance_for_toss_win", mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])
    home_away_stats.write.jdbc(dbinfo["DATABASE_URI"], "home_away_stats",
                               mode="overwrite", properties=dbinfo["DATABASE_PROPERTIES"])

    print('\n\n--------------------------------------------')
    print('---------- Writing Data Finished -----------')
    print('--------------------------------------------\n\n')
