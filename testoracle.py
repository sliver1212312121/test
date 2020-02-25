

# from datetime import datetime, timedelta
# import petl as etl
# import psycopg2
import os
# import pandas as pd
# import mysql.connector

import cx_Oracle

# connection = cx_Oracle.connect('username/password@IP:Port/DatabaseName')
# connection = cx_Oracle.connect('SPATIAL/LaItApS@10.3.33.6:3333/Profile_DOAE')
# print("connection success")


connection =cx_Oracle.connect('SPATIAL', 'LaItApS', "10.3.33.6:3333/doaedb",encoding="UTF-8")
connection.cursor()

SQL_Query = pd.read_sql_query(
 '''select * from V_KS01 fetch first 1 row only''', connection)

df = pd.DataFrame(SQL_Query)

master = df
master
# connection1 = mysql.connector.connect(
#     host = "203.154.28.234",
#     user = "sarayuth",
#     passwd = "3qZGA1QxqNp4PVpR",
#     db = "doae_production_2018"
#     )
# connection1.cursor()
#
# SQL_Query = pd.read_sql_query(
#  '''SELECT * form_plant_1 limit 10''', connection1)
#
# df = pd.DataFrame(SQL_Query)
#
# master = df
# master