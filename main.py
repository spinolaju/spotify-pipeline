import config

#Importing libraries
import requests
import pandas as pd
import json
import sqlalchemy
import sqlite3


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

### Extracting the data from the Spotify API

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
    record['name'] = album['name']
    record['release_date'] = album['release_date']
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
    record['name'] = track['name']
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

#Transforming the data into a tabular format
albums_df = pd.DataFrame(album_list, columns = ["id", "name", "release_date", "total_tracks", "artist_id"])
tracks_df = pd.DataFrame(albums_tracks, columns = ["album_id", "track_id", "name", "duration_ms", "loudness", "artist_id"])

#Validating Dataframes
if validate_data(albums_df, "id" ):
  print("Albums list dataframe validation ok")

if validate_data(tracks_df, "track_id"):
  print("Tracks list dataframe validation ok")


###Loading the processed data into the database

eng = sqlalchemy.create_engine(config.DB_LOCATION)
connection = sqlite3.connect('artist_report.sqlite')
c = connection.cursor()

#Creating queries to either create a table or append to a existing table

album_query = """
CREATE TABLE IF NOT EXISTS artist_albums(
    id VARCHAR(200),
    name VARCHAR(200), 
    release_date VARCHAR(200), 
    total_tracks VARCHAR(200),
    artist_id VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (id)
)
"""

track_query = """
CREATE TABLE IF NOT EXISTS tracks(
    album_id VARCHAR(200),
    track_id VARCHAR(200), 
    name VARCHAR(200), 
    duration_ms VARCHAR(200), 
    loudness VARCHAR(200),
    artist_id VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (track_id)
)
"""

c.execute(album_query)
c.execute(track_query)

try:
   albums_df.to_sql("artist_albums", eng, index=False, if_exists='append')
   tracks_df.to_sql("tracks", eng, index=False, if_exists='append')
except Exception as e:
    print(e)

connection.close()


