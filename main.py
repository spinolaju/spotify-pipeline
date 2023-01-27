# This code contains functions and commands used to extract data from the Spotify API and load it into a database.
# The first step is to get the access token from the Spotify API. This is done by making a POST request to the API with the client ID and client secret stored in the config module.
# Once the authentication is successful, the code starts by asking for the artist's name to search for. It then makes a GET request to the API with the name and type of the artist. 
# This returns a list of artists that match the search criteria, which is displayed using the tabulate method. The user is asked to choose one artist from the list and 
# the artist's ID is stored in a variable. 
# The next step is to get the artist's albums. This is done by making a GET request to the API with the artist's ID. Each album is then added to a dictionary. 
# If the album has more than 50 tracks, the code makes multiple requests with a limit of 50 and an offset to get all the tracks. Each track is added to a dictionary and the track's 
# loudness is then retrieved by making a GET request to the API with the track's ID. 
# Once all the data has been collected, 2 dataframes are created and validated. The dataframes are then load into the database in the form of two tables: 
# one containing the information about the artist albums and the other containing the information of the tracks associated with each album. 
# Finally, the code queries the database to retrieve album length and loudness.

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
from tabulate import tabulate
from alive_progress import alive_bar

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
      
def query_database(album_info, table_column, dict_to_append):
    for ai in album_info: 
        query = f"SELECT {table_column} FROM track WHERE album_id = '{ai['album_id']}' "
        query_response = connection.execute(query)
        for qr in query_response:
            records = {}
            records['album_id'] = str(ai['album_id']).replace('(', '').replace(',', '').replace(')', '')
            records[f'{table_column}'] = str(qr).replace('(', '').replace(',', '').replace(')', '').replace("'", '')
            dict_to_append.append(records)

### EXTRACTING AND PREPROCESSING

#Searching for the artist
while True:
  try:
    artist_name = input("Enter the artist's name: ")
    if not artist_name:
        raise ValueError('empty string')
  except ValueError as e:
    print("It looks like you didn't enter anything. Please enter an artist name.")
    continue
  else:
    break

get_artists = requests.get(
    f'{base_url}search',
    headers=headers, 
    params={ 'q': artist_name, 'type': 'artist' })

artist_query_resp = get_artists.json()

artist_query_results = []

for art in artist_query_resp['artists']['items']:
      record = {}
    
      record['artist_name'] = art['name']
      record['artist_id'] = art['id']
      record['followers'] = art['followers']['total']
      record['genres'] =  str(art['genres']).replace('[', '').replace(']', '')
      artist_query_results.append(record)

artist_query_df = pd.DataFrame(artist_query_results, columns = ["artist_name", "genres", "followers"])

#Displaying the artists found
print(tabulate(artist_query_df, showindex=True, headers=artist_query_df.columns))

while True:
    selectedNumber = input("Enter the index number of the artist you would like to select: ")
    if selectedNumber.isnumeric():
        selectedNumber = int(selectedNumber)
        try:
            artist = artist_query_results[selectedNumber]
        except IndexError:
            print("Error: You have entered an invalid index number. Please enter a valid index number.")
            continue
        else:
            print(f"Artist selected: {artist['artist_name']}")
            artist_id = artist['artist_id']
            break
    else:
        print("Error: You have entered an invalid index number. Please enter a valid index number.")
      
#Getting artist albums

get_albums = requests.get(f"{base_url}artists/{artist_id}/albums", 
                 headers=headers, 
                 params={'include_groups': 'album', 'limit': 50})

resp_albums = get_albums.json()

album_list = []
total = len(resp_albums['items'])

#Adding each album to a dictionary
with alive_bar(total, title='Getting albums') as bar:
  for album in resp_albums['items']:
    record = {}
    record['artist_id'] = artist_id
    record['id'] = album['id']
    record['album_name'] = album['name']
    record['release_year'] = parsing_date(album['release_date']) #parsing date 
    record['total_tracks'] = album['total_tracks']
    album_list.append(record)  
    bar()

#Getting album tracks
albums_tracks = []
resp_tracks = {}

for album_id in album_list:
    if album_id['total_tracks'] > 50:
        count = round(album_id['total_tracks']/ 50 + 0.5)
        offset = 0
        while count > 0:
          get_tracks = requests.get(f"{base_url}albums/{album_id['id']}/tracks?limit=50&offset={str(offset)}", headers=headers)
          resp_tracks.update(get_tracks.json())

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
              bar()
          offset += 50
          count -= 1
              
              
    else:
          get_tracks = requests.get(f"{base_url}albums/{album_id['id']}/tracks?limit=50", headers=headers)
          resp_tracks.update(get_tracks.json())
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
total = len(albums_tracks)
with alive_bar(total, title='Analysing tracks') as bar:
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
    bar()

#Adding track loudness to each track
for tr_info in track_info:
  for track in albums_tracks:
    if(tr_info['track_id'] == track['track_id']):
      track['loudness'] = tr_info['loudness']

#Validating Dataframes
with alive_bar(3, manual=True) as bar:
    print('Creating Dataframes')
    albums_df = pd.DataFrame(album_list, columns = ["id", "album_name", "release_year", "total_tracks", "artist_id"])                          
    tracks_df = pd.DataFrame(albums_tracks, columns = ["album_id", "track_id", "track_name", "duration_ms", "loudness", "artist_id"])
    bar(0.3)  
    print('Validating dataframes')                         
    if validate_data(albums_df, "id" ):
      print("Albums list dataframe validation ok")
    bar(0.6)                           
    if validate_data(tracks_df, "track_id"):
      print("Tracks list dataframe validation ok")
    bar(1.)     

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
  
  if(does_table_exist(connection, 'artist_album') == False):
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

  if(does_table_exist(connection, 'track') == False):
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
      
id_query = f"""SELECT id, album_name, total_tracks FROM artist_album WHERE artist_id = '{artist_id}'"""
album_ids = []
album_duration = []
album_loudness = []
with engine.connect() as connection:

    result = connection.execute(id_query)
    
    for r in result:
        temp = {}
        temp['album_id'] = str(r[0]).replace('(', '').replace(',', '').replace(')', '')
        temp['album_name'] = str(r[1]).replace('(', '').replace(',', '').replace(')', '')
        temp['total_tracks'] = str(r[2]).replace('(', '').replace(',', '').replace(')', '')
        temp['duration_total'] = 0
        album_ids.append(temp)
    
    loudness = 'loudness'
    duration_ms = 'duration_ms'
    query_database(album_ids, loudness, album_loudness)
    query_database(album_ids, duration_ms, album_duration)

    

for id in album_ids:
   count = 0
   for ad in album_duration:
     if ad['album_id'] == id['album_id']:
        count += float(ad['duration_ms'])
   id['duration_total'] = count/60000


albums_duration_df = pd.DataFrame(album_ids, columns = ["album_name", "duration_total", "total_tracks", "album_id"])

print("""
-----------------------------------------------------------------------------------------------------------------------------------
|                                                          Q U E R I E S                                                          | 
-----------------------------------------------------------------------------------------------------------------------------------
""")

print("1. The longest album in minutes:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total'], ascending=False).head(1), showindex=False, headers=albums_duration_df.columns))
print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("2. The shortest album in minutes:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total'], ascending=False).tail(1), showindex=False, headers=albums_duration_df.columns))
print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("3. All albums listed from the longest to the shortest:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total'], ascending=False), showindex=False, headers=albums_duration_df.columns))

for id in album_ids:
   temp = 0
   for ad in album_loudness:
    
     if ad['album_id'] == id['album_id']:
        temp += float(ad['loudness'])
      
   id['loudness'] = temp / float(id['total_tracks'])

album_loudness_df = pd.DataFrame(album_ids, columns = ["album_name", "loudness", "total_tracks", "album_id"])


print("""
-----------------------------------------------------------------------------------------------------------------------------------
The overall loudness of album tracks in decibels (dB). Loudness values are averaged across the entire track and are useful for comparing
relative loudness of tracks. Loudness is the quality of a sound that is the primary psychological correlate of physical strength 
(amplitude). Values typically range between -60 and 0 db.
-----------------------------------------------------------------------------------------------------------------------------------
""")


print("4. The average'loudest' album in decibels (dB):")
print(tabulate(album_loudness_df.sort_values(by=['loudness'], ascending=False).head(1), showindex=False, headers=album_loudness_df.columns))

print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("5. The average 'quietest' album in decibels (dB):")
print(tabulate(album_loudness_df.sort_values(by=['loudness'], ascending=False).tail(1), showindex=False, headers=album_loudness_df.columns))

print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("6. The overall loudness of albums in decibels (dB). All albums listed from the 'loudest' to the 'quietest':")
print(tabulate(album_loudness_df.sort_values(by=['loudness'], ascending=False), showindex=False, headers=album_loudness_df.columns))

  