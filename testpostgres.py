

from datetime import datetime, timedelta
import petl as etl
import psycopg2
import os
import pandas as pd
import mysql.connector

connection2 = psycopg2.connect(
         host= '10.3.33.77',
         dbname= 'data_tr',
         user= 'tr',
         password= 'TR@1234',
         port= '5432'
     )
connection2.cursor()

SQL_Query = pd.read_sql_query(
    '''SELECT * FROM "X08_01_03_Weather_nectec_name" limit 10''', connection2)

df = pd.DataFrame(SQL_Query)

master = df
print(master)
