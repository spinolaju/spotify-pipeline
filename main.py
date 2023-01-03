import config

'''importing libraries'''
import requests
import pandas as pd
import json
import sqlalchemy
import sqlite3



AUTH_URL = 'https://accounts.spotify.com/api/token'

#getting access token
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': config.CLIENT_ID,
    'client_secret': config.CLIENT_SECRET,
})

#variables
base_url = 'https://api.spotify.com/v1/'
artist_id = '51Blml2LZPmy7TTiAg47vQ'

auth_response_data = auth_response.json()

access_token = auth_response_data['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}



#getting artist albums

get_albums = requests.get(f"{base_url}artists/{artist_id}/albums", 
                 headers=headers, 
                 params={'include_groups': 'album', 'limit': 50})

resp_albums = get_albums.json()

album_list = []

for album in resp_albums['items']:
    record = {}
    record['id'] = album['id']
    record['name'] = album['name']
    record['available_markets'] = album['available_markets']
    record['release_date'] = album['release_date']
    record['release_date'] = album['release_date']
    record['total_tracks'] = album['total_tracks']
    album_list.append(record)

#pd.json_normalize(album_list)

#getting album tracks
track_list = []

for album_id in album_list:
   get_tracks = requests.get(f"{base_url}albums/{album_id['id']}/tracks", headers=headers)
   
   resp_tracks = get_tracks.json()
   for track in resp_tracks['items']:
    record = {}
    record['album_id'] = album_id['id']
    record['id'] = track['id']
    record['name'] = track['name']
    record['track_number'] = track['track_number']
    record['duration_ms'] = track['duration_ms']
    track_list.append(record)
    
  
#Getting Audio analysis for each track

track_info = []

for track_id in albums_tracks:
   get_tracks_analysis = requests.get(f"{base_url}audio-analysis/{track_id['track_id']}", headers=headers)

   resp_tr_analysis = get_tracks_analysis.json()

   for tr_attr, tr_info in resp_tr_analysis['track'].items():
    

    record = {}
    if tr_attr == "loudness":
     record['track_id'] = track_id['track_id']
     record['loudness'] = tr_info
     track_info.append(record)
