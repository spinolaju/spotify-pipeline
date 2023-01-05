import config

#Importing libraries
import requests
import pandas as pd
import json
import pyodbc
import sqlalchemy
import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from datetime import datetime
    

server = config.SERVER
database = config.DATABASE
username = config.USERNAME
password = config.PASSWORD
AUTH_URL = 'https://accounts.spotify.com/api/token'

#Getting access token
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': config.CLIENT_ID,
    'client_secret': config.CLIENT_SECRET,
})

#Defining Variables
base_url = 'https://api.spotify.com/v1/'
artist_id = '51Blml2LZPmy7TTiAg47vQ'

auth_response_data = auth_response.json()

access_token = auth_response_data['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}


#Defining Functions

def validate_data(df: pd.DataFrame, id) -> bool:

    #Checking if Primary key is unique
    if pd.Series(df[id]).is_unique:
        pass
    else:
      raise Exception("The Primary Key is invalid")

    #Checking for nulls
    if df.isnull().values.any():
      raise Exception("Null values found")
    return True

def parsing_date(date_string):
    datetime_object = datetime.strptime(date_string, '%Y-%m-%d' )
    return datetime_object.strftime('%Y')


def does_table_exist(db_con, table_name):
    #c = db_con.cursor()
    result = db_con.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """.format(table_name.replace('\'', '\'\'')))
    
    if(result.fetchone()[0] == 1):
        return True
    else:
        return False

### EXTRACTING AND PREPROCESSING
#Getting artist albums

get_albums = requests.get(f"{base_url}artists/{artist_id}/albums", 
                 headers=headers, 
                 params={'include_groups': 'album', 'limit': 50})

resp_albums = get_albums.json()

album_list = []

#Adding each album to a dictionary

for album in resp_albums['items']:
    record = {}
    record['artist_id'] = artist_id
    record['id'] = album['id']
    record['album_name'] = album['name']
    record['release_year'] = parsing_date(album['release_date']) #parsing date 
    record['total_tracks'] = album['total_tracks']
    album_list.append(record)

#Getting album tracks
albums_tracks = []

for album_id in album_list:
   get_tracks = requests.get(f"{base_url}albums/{album_id['id']}/tracks", headers=headers)
   
   resp_tracks = get_tracks.json()

#Adding each track to a dictionary
   for track in resp_tracks['items']:
    record = {}
    record['album_id'] = album_id['id']
    record['track_id'] = track['id']
    record['track_name'] = track['name']
    record['track_number'] = track['track_number']
    record['duration_ms'] = track['duration_ms']
    record['loudness'] = ""
    record['artist_id'] = artist_id
    albums_tracks.append(record)
    
#Getting Audio analysis for each track

track_info = []

for track_id in albums_tracks:
   get_tracks_analysis = requests.get(f"{base_url}audio-analysis/{track_id['track_id']}", headers=headers)

   resp_tr_analysis = get_tracks_analysis.json()
#Appending track loudness to each track in the dictionary
   for tr_attr, tr_info in resp_tr_analysis['track'].items():
      record = {}
      if tr_attr == "loudness":
        record['track_id'] = track_id['track_id']
        record['loudness'] = tr_info
        track_info.append(record)

#Adding track loudness to each track
for tr_info in track_info:
  for track in albums_tracks:
    if(tr_info['track_id'] == track['track_id']):
      track['loudness'] = tr_info['loudness']

albums_df = pd.DataFrame(album_list, columns = ["id", "album_name", "release_year", "total_tracks", "artist_id"])
tracks_df = pd.DataFrame(albums_tracks, columns = ["album_id", "track_id", "track_name", "duration_ms", "loudness", "artist_id"])

#Validating Dataframes
if validate_data(albums_df, "id" ):
  print("Albums list dataframe validation ok")

if validate_data(tracks_df, "track_id"):
  print("Tracks list dataframe validation ok")


###LOADING THE PROCESSED DATA INTO THE DATABASE 

#Connecting to the database

params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                     "SERVER="+server+";"
                                     "DATABASE="+database+";"
                                     "UID="+username+";"
                                     "PWD="+password+";")
    
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

#Creating tables
with engine.connect() as connection:

  if(does_table_exist(connection, 'artist_album')):
    print("artist_album table exists")
  else:
    album_query = """
    CREATE TABLE artist_album(
    id VARCHAR(200) primary key, 
    album_name VARCHAR(200), 
    release_year VARCHAR(200), 
    total_tracks VARCHAR(200),
    artist_id VARCHAR(200)
    )
    """
    connection.execute(album_query)

  if(does_table_exist(connection, 'track')):
    print("track table exists")

  else:
    track_query = """
    CREATE TABLE track(
    album_id VARCHAR(200),
    track_id VARCHAR(200) primary key, 
    track_name VARCHAR(200), 
    duration_ms VARCHAR(200), 
    loudness VARCHAR(200),
    artist_id VARCHAR(200)
    )
    """
    connection.execute(track_query)


  try:
  #Creating queries to either insert/append to a table
    albums_df.to_sql("album_temp", index=False, schema = 'dbo', con=engine, if_exists='replace')
    print('Loading album dataframe into the database')
    tracks_df.to_sql("track_temp", index=False, schema = 'dbo', con=engine, if_exists='replace')
    print('Loading tracks dataframe into the database')
      

    album_table = """INSERT INTO artist_album (id, album_name, release_year, total_tracks, artist_id)
            SELECT t.id, t.album_name, t.release_year, t.total_tracks, t.artist_id
            FROM album_temp t
            WHERE NOT EXISTS 
                (SELECT 1 FROM artist_album f
                 WHERE t.id = f.id)
                 """
    print('Updating tables...')
    track_table = """INSERT INTO track (album_id, track_id, track_name, duration_ms, loudness, artist_id)
            SELECT t.album_id, t.track_id, t.track_name, t.duration_ms, t.loudness, t.artist_id
            FROM track_temp t
            WHERE NOT EXISTS 
                (SELECT 1 FROM track f
                 WHERE t.track_id = f.track_id)
                 """
  #Loading the data into the correspondent tables
    connection.execute(album_table)
    connection.execute(track_table)
    print('Data loaded into the database successfully!')

  except Exception as e:
      print(e)

