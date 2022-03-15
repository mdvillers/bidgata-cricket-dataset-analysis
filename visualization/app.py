from flask import Flask, render_template
import psycopg2


app = Flask(__name__)

app = Flask(__name__)

app.config.from_pyfile('config.py')


DB_HOST = app.config['DB_HOST']
DB_NAME = app.config['DB_NAME']
DB_USER = app.config['DB_USER']
DB_PASSWORD = app.config['DB_PASSWORD']


def get_db_connection():
    conn = psycopg2.connect(host=DB_HOST,
                            database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASSWORD,
                            )
    return conn


def get_most_winning_team():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM most_winning_team;')
    most_winning_team = cur.fetchall()
    cur.close()
    conn.close()
    return most_winning_team[0][0]


def get_batsman_strike_rate():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM batsman_strike_rate;')
    batsman_strike_rate = cur.fetchall()
    cur.close()
    conn.close()
    return batsman_strike_rate


def get_bowler_economy_rate():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM bowler_economy_rate;')
    bowler_economy_rate = cur.fetchall()
    cur.close()
    conn.close()
    return bowler_economy_rate


def get_home_away_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM home_away_stats;')
    home_away_stats = cur.fetchall()
    cur.close()
    conn.close()
    return home_away_stats


def get_player_teams_count():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM player_teams_count;')
    player_teams_count = cur.fetchall()
    cur.close()
    conn.close()
    return player_teams_count


def get_season_batsman_stats_2017():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM season_batsman_stats_2017;')
    season_batsman_stats_2017 = cur.fetchall()
    cur.close()
    conn.close()
    return season_batsman_stats_2017


def get_season_batsman_percentage_growth_2016_2017():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM season_batsman_percentage_growth_2016_2017;')
    season_batsman_percentage_growth_2016_2017 = cur.fetchall()
    cur.close()
    conn.close()
    return season_batsman_percentage_growth_2016_2017


def get_season_bowler_stats_2017():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM season_bowler_stats_2017;')
    season_bowler_stats_2017 = cur.fetchall()
    cur.close()
    conn.close()
    return season_bowler_stats_2017


def get_season_bowler_economy_stats_2017():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM season_bowler_economy_stats_2017;')
    season_bowler_economy_stats_2017 = cur.fetchall()
    cur.close()
    conn.close()
    return season_bowler_economy_stats_2017


def get_winning_chance_for_toss_win():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM winning_chance_for_toss_win;')
    winning_chance_for_toss_win = cur.fetchall()
    cur.close()
    conn.close()
    return winning_chance_for_toss_win[0][0]


@app.route('/')
def index():
    most_winning_team = get_most_winning_team()
    batsman_strike_rate = get_batsman_strike_rate()
    bowler_economy_rate = get_bowler_economy_rate()
    home_away_stats = get_home_away_stats()
    player_teams_count = get_player_teams_count()
    season_batsman_stats_2017 = get_season_batsman_stats_2017()
    season_batsman_percentage_growth_2016_2017 = get_season_batsman_percentage_growth_2016_2017()
    season_bowler_economy_stats_2017 = get_season_bowler_economy_stats_2017()
    season_bowler_stats_2017 = get_season_bowler_stats_2017()
    winning_chance_for_toss_win = get_winning_chance_for_toss_win()

    return render_template('index.html', most_winning_team=most_winning_team,
                           batsman_strike_rate=batsman_strike_rate,
                           bowler_economy_rate=bowler_economy_rate,
                           home_away_stats=home_away_stats,
                           player_teams_count=player_teams_count,
                           season_batsman_stats_2017=season_batsman_stats_2017,
                           season_batsman_percentage_growth_2016_2017=season_batsman_percentage_growth_2016_2017,
                           season_bowler_stats_2017=season_bowler_stats_2017,
                           season_bowler_economy_stats_2017=season_bowler_economy_stats_2017,
                           winning_chance_for_toss_win=winning_chance_for_toss_win
                           )


# API Runs on port 5000
if __name__ == '__main__':
    app.run(port=5000, debug=True)
