

from datetime import datetime, timedelta
import petl as etl
import psycopg2
import os
import pandas as pd
import mysql.connector


connection1 = mysql.connector.connect(
    host = "203.154.28.234",
    user = "sarayuth",
    passwd = "3qZGA1QxqNp4PVpR",
    db = "doae_production_2018"
    )
connection1.cursor()

SQL_Query = pd.read_sql_query(
 '''SELECT
 `t`.`c_year`,
--  `t`.`db_year`,
 `t`.`db_month`,
-- `t`.`db_week`,
 `t`.`code_province_id`,
 `t`.`code_amphur_id`,
 `t`.`code_tambon_id`, 
 `t`.`product_group_id`,
 `t`.`product_id`,
 `t`.`product_breed_id`,
 `t`.`db_season`,
 SUM( t.area_plant ) AS `area_plant`,
 SUM( t.area_harvest ) AS `area_harvest`,
 SUM( t.area_blooey ) AS `area_blooey`,
 SUM( t.harvest_produce ) AS `product_harvest`,
IF
 (
  SUM( t.area_harvest )= 0,
  NULL,
 ROUND( SUM( t.harvest_produce )/ SUM( t.area_harvest ), 2 )) AS `product_avg`,
IF
 (
  SUM( t.harvest_produce ) > 0,
  ROUND( SUM( t.price_factor ) / SUM( t.harvest_produce ), 2 ),
 NULL 
 ) AS `product_price`,
 `master_tambon`.`id` AS `loc_id`,
 `master_tambon`.`name` AS loc_name 
FROM
 (
 SELECT
  `p`.`c_process_date`,
  `p`.`c_year`,
  `p`.`db_year`,
  `p`.`db_month`,
  `p`.`db_week`,
  `p`.`db_season`,
  `p`.`rice_type_id` AS `product_type_id`,
  ( 0 ) AS `code_country_id`,
  `p`.`code_region_id`,
  `p`.`code_zone_id`,
  `p`.`code_watershed_id`,
  `p`.`code_province_id`,
  `p`.`code_amphur_id`,
  `p`.`code_tambon_id`,
  `p`.`product_group_id`,
  `p`.`product_id`,
  `p`.`product_breed_id`,
  SUM( p.c_area_blooey_in ) AS `area_blooey_in`,
  SUM( p.c_area_blooey_ex ) AS `area_blooey_ex`,
  SUM( p.c_area_blooey_in + p.c_area_blooey_ex ) AS `area_blooey`,
  SUM( p.c_area_harvest_in ) AS `area_harvest_in`,
  SUM( p.c_area_harvest_ex ) AS `area_harvest_ex`,
  SUM( p.c_area_harvest_in + p.c_area_harvest_ex ) AS `area_harvest`,
  SUM( p.c_harvest_produce_in ) AS `harvest_produce_in`,
  SUM( p.c_harvest_produce_ex ) AS `harvest_produce_ex`,
  SUM( p.c_harvest_produce ) AS `harvest_produce`,
  AVG( p.c_price_produce ) AS `price_produce`,
  SUM( p.c_harvest_produce * p.c_price_produce ) AS `price_factor`,
  SUM( p.c_area_new_in ) AS `area_new_in`,
  SUM( p.c_area_new_ex ) AS `area_new_ex`,
  SUM( p.c_area_new_in + p.c_area_new_ex ) AS `area_new`,
  SUM(
  IF
   ( p.plant_month = 1 AND p.db_week = IF ( p.d_year, 1, 4 ), p.c_area_first_month_ex, 0 ) + p.c_area_new_ex 
  ) AS `area_plant_ex`,
  SUM(
  IF
   ( p.plant_month = 1 AND p.db_week = IF ( p.d_year, 1, 4 ), p.c_area_first_month_in, 0 ) + p.c_area_new_in 
  ) AS `area_plant_in`,
  SUM(
  IF
   ( p.plant_month = 1 AND p.db_week = IF ( p.d_year, 1, 4 ), p.c_area_first_month, 0 ) + p.c_area_new 
  ) AS area_plant 
 FROM
  form_plant_1 p
  INNER JOIN form_data b ON b.id = p.id 
 WHERE
  (
  `b`.`record_status` IN ( 40, 30, 20 )) 
  AND ( `p`.`c_year` >= '2009' ) AND ( `p`.`c_year` <= '2019' ) 
  AND ( `p`.`product_id` = '020500' )  
 GROUP BY
  `p`.`code_province_id`,
  `p`.`code_amphur_id`,
  `p`.`code_tambon_id`,
  `p`.`product_group_id`,
  `p`.`product_id`,
  `p`.`product_breed_id`,
  `p`.`db_season` ,
  `p`.`c_year`,
  `p`.`db_month`
 ) t
 INNER JOIN master_tambon ON master_tambon.id = t.code_tambon_id 
GROUP BY
 `t`.`code_province_id`,
 `t`.`code_amphur_id`,
 `t`.`code_tambon_id`,
 `t`.`c_year`,
 `t`.`db_month`
ORDER BY
 `t`.`code_tambon_id`''', connection1)
   
df = pd.DataFrame(SQL_Query)   

master = df

master['product_price'] = master['product_price'].fillna(master.groupby(['code_tambon_id', 'c_year', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_amphur_id', 'c_year', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_province_id', 'c_year', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_tambon_id', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_amphur_id', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_province_id', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['c_year', 'db_month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['c_year'])['product_price'].transform('mean'))

# Create full date every 1H
date_gen = pd.DataFrame(pd.date_range(start='01/01/2011', end='31/12/2019', freq='M'), columns=['Date'])
# NOT NULL DATA
date_gen['year'] = date_gen['Date'].dt.year
date_gen['month'] = date_gen['Date'].dt.month
a = master['code_tambon_id'].unique().tolist()
list_year = []
list_month = []
list_tanmbonID = []
for i in range(len(a)):
    for j in range(len(date_gen)):
        date_gen['tambonID'] = a[i]
        year = date_gen['year']
        month = date_gen['month']
        tambonID = date_gen['tambonID']
        list_year.append(year[j])
        list_month.append(month[j])
        list_tanmbonID.append(tambonID[j])
    
DataFull = pd.DataFrame({
    'year': list_year,
    'month': list_month,
    'tambonID': list_tanmbonID
})
DataFull
    
# make string version of original column, call it 'col'
DataFull['tambonID'] = DataFull['tambonID'].astype(str)
# make the new columns using string indexing
DataFull['code_province_id'] = DataFull['tambonID'].str[0:2]
DataFull['code_amphur_id'] = DataFull['tambonID'].str[2:6]
DataFull['code_province_id'] = DataFull['code_province_id'].astype(int)
DataFull['code_amphur_id'] = DataFull['code_amphur_id'].astype(int)
DataFull['tambonID'] = DataFull['tambonID'].astype(int)


#    DataFull['product_price'] = DataFull['product_price'].astype(int)
#     DataFull['product_avg'] = DataFull['product_avg'].astype(int)
#     DataFull['product_harvest'] = DataFull['product_harvest'].astype(int)
#     DataFull['tambonID'] = DataFull['tambonID'].astype(int)
#     DataFull['tambonID'] = DataFull['tambonID'].astype(int)


master = master.loc[ (master['product_price'] > 0) & (master['product_price'] < 6000) & (master['product_harvest'] < 400000000) & ( master['area_harvest'] < 100000) & (master['area_plant'] > 0) & (master['area_plant'] < 150000) & (master['product_avg'] < 1000000)]
master['product_harvest'].fillna(0, inplace=True)
# master = master[['c_year','db_month','code_tambon_id','area_plant','area_harvest','area_blooey','product_harvest','product_avg','product_price']]
fillmissMonth = DataFull.merge(master, how='left', left_on=['year', 'month', 'tambonID'],right_on=['c_year', 'db_month', 'code_tambon_id'])
# fillmissMonth = fillmissMonth[['year','month','tambonID','area_plant','area_harvest','area_blooey','product_harvest','product_avg','product_price']]
# fillmissMonth.fillna(0,inplace=True)
master = fillmissMonth
    
master['tambonID'] = master['tambonID'].astype(str)
master['code_amphur_id_x'] = master['code_amphur_id_x'].astype(str)
master['code_province_id_x'] = master['code_province_id_x'].astype(str)
    
master['product_price'] = master['product_price'].fillna(master.groupby(['tambonID', 'year', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_amphur_id_x', 'year', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_province_id_x', 'year', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['tambonID', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_amphur_id_x', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['code_province_id_x', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['year', 'month'])['product_price'].transform('mean'))
master['product_price'] = master['product_price'].fillna(master.groupby(['year'])['product_price'].transform('mean'))
    
master['product_avg'] = master['product_avg'].fillna(master.groupby(['tambonID', 'year', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['code_amphur_id_x', 'year', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['code_province_id_x', 'year', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['tambonID', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['code_amphur_id_x', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['code_province_id_x', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['year', 'month'])['product_avg'].transform('mean'))
master['product_avg'] = master['product_avg'].fillna(master.groupby(['year'])['product_avg'].transform('mean'))

master['tambonID'] = master['tambonID'].astype(int)
master.fillna(0,inplace=True)
master = master[['year', 'month', 'tambonID', 'area_plant', 'area_harvest', 'area_blooey',
       'product_harvest', 'product_avg', 'product_price']]


table2 = etl.fromdataframe(master)




connection2 = psycopg2.connect(
         host= '10.3.33.77',
         dbname= 'data_tr',
         user= 'tr',
         password= 'TR@1234',
         port= '5432'
     )
etl.todb(table2, connection2, 'X04')

