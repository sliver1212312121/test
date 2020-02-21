

from datetime import datetime, timedelta
import petl as etl
import psycopg2
import os
import pandas as pd
import mysql.connector
print("hello")

db = mysql.connector.connect(
  host="203.154.28.234",
  user="sarayuth",
  passwd="3qZGA1QxqNp4PVpR",
  db="doae_production_2018")
print(db)
#
# SQL_Query = pd.read_sql_query(
#     '''SELECT * FROM form_plant_1 limit 10''', connection1)
#
# df = pd.DataFrame(SQL_Query)
#
# master = df
# master
