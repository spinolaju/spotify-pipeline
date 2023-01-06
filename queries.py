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

from datetime import datetime

server = config.SERVER
database = config.DATABASE
username = config.USERNAME
password = config.PASSWORD

params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                     "SERVER="+server+";"
                                     "DATABASE="+database+";"
                                     "UID="+username+";"
                                     "PWD="+password+";")

engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))     



id_query = """SELECT id, album_name, total_tracks FROM artist_album"""
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
    

    for aid in album_ids: 
        album_duration_query = f"SELECT duration_ms FROM track WHERE album_id = '{aid['album_id']}' "
        duration_results = connection.execute(album_duration_query)
        for dr in duration_results:
            records = {}
            records['album_id'] = str(aid['album_id']).replace('(', '').replace(',', '').replace(')', '')
            records['duration_ms'] = str(dr).replace('(', '').replace(',', '').replace(')', '').replace("'", '')
            album_duration.append(records)

    for loud in album_ids: 
        album_loudness_query = f"SELECT loudness FROM track WHERE album_id = '{loud['album_id']}' "
        loudness_res = connection.execute(album_loudness_query)
        for lr in loudness_res:
            records = {}
            records['album_id'] = str(loud['album_id']).replace('(', '').replace(',', '').replace(')', '')
            records['loudness'] = str(lr).replace('(', '').replace(',', '').replace(')', '').replace("'", '')
            album_loudness.append(records)

count = 0


for id in album_ids:
   count = 0
   for ad in album_duration:
     if ad['album_id'] == id['album_id']:
        count += float(ad['duration_ms'])
   id['duration_total'] = count/60000


albums_duration_df = pd.DataFrame(album_ids, columns = ["album_name", "duration_total", "album_id"])

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

  