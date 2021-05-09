# -*- coding: utf-8 -*-
"""
Created on Wed May  5 18:40:42 2021

@author: parjag
"""

import psycopg2
import csv
import pandas as pd

#establishing the connection
conn = psycopg2.connect(
   database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
)
cursor = conn.cursor()

sql="SELECT json_agg(row_to_json((SELECT ColumnName FROM (SELECT HAP.country,HAP.HIGHEST_RANK, HAP.LOWEST_RANK, HAP.HAPPINESS_SCORE)AS ColumnName (country,HIGHEST_RANK,LOWEST_RANK, HAPPINESS_SCORE)))) AS JsonData FROM(SELECT MAX(cast(region_code as integer)) HIGHEST_RANK,MIN(cast(region_code as integer)) LOWEST_RANK,HAPPINESS_SCORE,country FROM WH.COUNTRY_HAPPINESS_FACT A INNER JOIN WH.COUNTRY_DIMENSION B ON A.COUNTRY_DIMENSION_code=B.country GROUP BY country, region_code, HAPPINESS_SCORE) AS HAP GROUP BY HAP.country;";

f=open('c:/Users/parjag/test/test.txt','w');
f.write(sql);
f.close();

  # Execute the SQL command
cursor.execute(sql)

with open("c:/Users/parjag/test/test.json","w",newline='') as f:
    for row in cursor:
        print (row,file=f)

# f=open('c:/Users/parjag/test/test.json','w');
# f.write(RES);
# f.close();

conn.commit()