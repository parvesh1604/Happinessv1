# -*- coding: utf-8 -*-
"""
Created on Thu May  6 19:39:52 2021

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

# Import CSV
# data = pd.read_csv (r'C:\Users\parjag\test\country_dim.csv')   
# df = pd.DataFrame(data, columns= ['COUNTRY_URL','REGION_CODE','RANK'])
# #Creating a cursor object using the cursor() method
# print("enter Date: ")
# Date1=input()


with open('C:/Users/parjag/test/2015.csv', 'r') as f:
    reader = csv.reader(f,delimiter=',')
    next(reader) # Skip the header row.
    for row in reader:
        # cur.execute(
        # "INSERT INTO WH.COUNTRY_DIMENSION (COUNTRY_URL, REGION_CODE, RANK) 
        # VALUES (%s, %s, %s)",
        # row
        
        # Prepare SQL query to INSERT a record into the database.
            sql = f"INSERT INTO WH.COUNTRY_HAPPINESS_FACT(country_dimension_code, calendar_date,happiness_score, economy_gdp, family, health_life_expectancy, perception_of_corruption, generosity, dystopia_residual, freedom) VALUES ('{row[0]}',20151231,'{row[1]}','{row[3]}','{row[4]}','{row[5]}','{row[7]}','{row[8]}','{row[9]}','{row[6]}')"
            
             # sql = f"INSERT INTO WH.COUNTRY_DIMENSION(REGION_CODE) VALUES ({row[1]});" 
            print(sql)
            try:
                # if row:
                #     r1= int(row[0])
               # Execute the SQL command
                cursor.execute(sql)
                                 
               
                print("success")
               # Commit your changes in the database
                conn.commit()
            except:
               # Rollback in case there is any error
               conn.rollback()
               print("error")
        
  

conn.commit()






#Closing the connection
conn.close()