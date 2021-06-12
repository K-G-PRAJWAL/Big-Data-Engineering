import sqlite3
import requests
import json
from datetime import datetime
import datetime
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import auth

# Transform
def check_if_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print('No songs downloaded.')
        return False

    # Check for primary key(played_at)
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key Check has failed.')

    # Check for null values
    if df.isnull().values.any():
        raise Exception('Null values found.')

    # Check if timestamps are all within the last 24 hours
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception(
                "At least one of the returned songs does not have a yesterday's timestamp.")

    return True

def run_spotify_etl():
    DATABASE_LOCATION = 'sqlite:///recently_played_tracks.sqlite'
    USER_ID = auth.USER_ID
    TOKEN = auth.TOKEN


    # Extract

    # headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # Get yesterday's timestamp
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    # Fetch all the songs played yesterday and convert them to JSON
    req = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix), headers=headers)
    data = req.json()

    # Fetch the required fields from the JSON data
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Convert these data into a pandas dataframe
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }
    song_df = pd.DataFrame(song_dict, columns=[
                           "song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)

    # Validation
    if check_if_valid(song_df):
        print("Data Valid.")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('recently_played_tracks.sqlite')
    print("Database Connected.")
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS recently_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(query)

    try:
        song_df.to_sql("recently_played_tracks", engine,
                       index=False, if_exists='append')
    except:
        print('Data already exists.')

    conn.close()
    print('Database connection closed.')