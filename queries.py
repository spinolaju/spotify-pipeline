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



id_query = """SELECT id, album_name FROM artist_album"""
album_ids = []
album_duration = []
with engine.connect() as connection:

    result = connection.execute(id_query)
    
    for r in result:
        temp = {}
        temp['album_id'] = str(r[0]).replace('(', '').replace(',', '').replace(')', '')
        temp['album_name'] = str(r[1]).replace('(', '').replace(',', '').replace(')', '')
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


#print(albums_duration_df.sort_values(by=['duration_total'], ascending=False).head(1).to_string(index=False))

print("1. The longest album in minutes:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total']).head(1), showindex=False, headers=albums_duration_df.columns))
print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("2. The shortest album in minutes:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total'], ascending=False).tail(1), showindex=False, headers=albums_duration_df.columns))
print()

print("-----------------------------------------------------------------------------------------------------------------------------------")
print("3. All albums listed from the longest to the shortest:")
print(tabulate(albums_duration_df.sort_values(by=['duration_total'], ascending=False), showindex=False, headers=albums_duration_df.columns))

