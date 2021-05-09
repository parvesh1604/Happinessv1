# -*- coding: utf-8 -*-
"""
Created on Sun May  9 11:52:45 2021

@author: parjag
"""

import psycopg2
import csv
import pandas as pd
# import Python's built-in JSON library
import json

# import the JSON library from psycopg2.extras
from psycopg2.extras import Json

# import psycopg2's 'json' using an alias
from psycopg2.extras import json as psycop_json

# import Python's 'sys' library
import sys
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import numpy as np
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

def insert_2015():
    
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

def insert_2016():
    #establishing the connection
    conn = psycopg2.connect(
    database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
    )
    cursor = conn.cursor()

    with open('C:/Users/parjag/test/2016.csv', 'r') as f:
       reader = csv.reader(f,delimiter=',')
       next(reader) # Skip the header row.
       for row in reader:
        
        
        # Prepare SQL query to INSERT a record into the database.
            sql = f"INSERT INTO WH.COUNTRY_HAPPINESS_FACT(country_dimension_code, calendar_date,happiness_score, economy_gdp, family, health_life_expectancy, perception_of_corruption, generosity, dystopia_residual, freedom,lower_confidence,upper_confidence) VALUES ('{row[0]}',20161231,'{row[1]}','{row[4]}','{row[5]}','{row[5]}','{row[6]}','{row[8]}','{row[9]}','{row[6]}','{row[2]}','{row[3]}')"
            
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



def insert_2017():
    #establishing the connection
    conn = psycopg2.connect(
   database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
   )
    cursor = conn.cursor()

    with open('C:/Users/parjag/test/2017.csv', 'r') as f:
        reader = csv.reader(f,delimiter=',')
        next(reader) # Skip the header row.
    for row in reader:
        
        # Prepare SQL query to INSERT a record into the database.
            sql = f"INSERT INTO WH.COUNTRY_HAPPINESS_FACT(country_dimension_code, calendar_date,happiness_score, economy_gdp, family, health_life_expectancy, perception_of_corruption, generosity, dystopia_residual, freedom,lower_confidence,upper_confidence) VALUES ('{row[0]}',20171231,'{row[1]}','{row[4]}','{row[5]}','{row[6]}','{row[9]}','{row[8]}','{row[10]}','{row[7]}','{row[3]}','{row[2]}')"
            
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
    
def insert_json():
  # establishing the connection
    conn = psycopg2.connect(
       database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
    )
    cursor = conn.cursor()
    
    # accept command line arguments for the Postgres table name
    if len(sys.argv) > 1:
        table_name = '_'.join(sys.argv[1:])
    else:
        # ..otherwise revert to a default table name
        table_name = "wh.country_dimension"
    
    # print ("\ntable name for JSON data:", table_name)
    
    with open('C:/Users/parjag/test/Files/Data_Files/Data Files/countries_continents_codes_flags_url.json') as json_data:
    
        # use load() rather than loads() for JSON files
        record_list = json.load(json_data)
        
    # print ("\nrecords:", record_list)
    # print ("\nJSON records object type:", type(record_list)) # should return "<class 'list'>"
    
    # concatenate an SQL string
    sql_string = 'INSERT INTO {} '.format( table_name )
    
    # if record list then get column names from first key
    if type(record_list) == list:
        first_record = record_list[0]
        columns = list(first_record.keys())
        
        print ("\ncolumn names:", columns)
    
    # if just one dict obj or nested JSON dict
    else:
        print ("Needs to be an array of JSON objects")
        sys.exit()
    
    # enclose the column names within parenthesis
    sql_string += "(" + ', '.join(columns) + ")\nVALUES "
    
    for i, record_dict in enumerate(record_list):
    
        # iterate over the values of each record dict object
        values = []
        for col_names, val in record_dict.items():
    
            # Postgres strings must be enclosed with single quotes
            if type(val) == str:
                # escape apostrophies with two single quotations
                val = val.replace("'", "''")
                val = "'" + val + "'"
    
            values += [ str(val) ]
    
        # join the list of values and enclose record in parenthesis
        sql_string += "(" + ', '.join(values) + "),\n"
    
    # remove the last comma and end statement with a semicolon
    sql_string = sql_string[:-2] + ";"
    sql_string = sql_string.replace("-", "_")
    sql_string = sql_string.replace("-", "_")
    sql_string = sql_string.replace("None", "null")
    
    print ("\nSQL string:")
    # print (sql_string)
    
    try:
     # if row:
     #     r1= int(row[0])
     # Execute the SQL command
     cursor.execute(sql_string)
                                     
     print("success")
     # Commit your changes in the database
     conn.commit()
    except:
     # Rollback in case there is any error
     conn.rollback()
     print("error")
     
def Modelling_record():
    # establishing the connection
    conn = psycopg2.connect(
       database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
    )
    cursor = conn.cursor()
    
    #Setting auto commit false
    conn.autocommit = True
    
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    #Retrieving data
    cursor.execute('''select SUBSTRING(cast(calendar_date as varchar),0,5) as year, dim.country, image_url country_url,region_code,case when (region='' or region is null) then 'Nan' else upper(region) end region, region_code Rank_Per_Region, sum(cast (region_code as integer)) Overall_Rank,happiness_score,case when happiness_score >5.6 then 'green' when happiness_score <2.6 then 'red' else 'amber' end Happiness_status,economy_gdp,family,social_support,health_life_expectancy,freedom,generosity,perception_of_corruption from wh.country_happiness_fact fct inner join wh.country_dimension dim On fct.country_dimension_code=dim.country group by calendar_date,dim.country, image_url,region_code,case when (region='' or region is null) then 'Nan' else upper(region) end , region_code,happiness_score,case when happiness_score >5.6 then 'green' when happiness_score <2.6 then 'red' else 'amber' end ,economy_gdp,family,social_support,health_life_expectancy,freedom,generosity,perception_of_corruption''')
    
    #Fetching 1st row from the table
    result = cursor.fetchone();
    print(result)
    
    #Fetching 1st row from the table
    result = cursor.fetchall();
    # print(result)
    
    print("Print each row and it's columns values")
    for row in result:
            print("Year: ",row[0])
            print("Country: ",row[1])
            print("Country Url: ",row[2])
            print("Region Code: ",row[3])
            print("Region: ",row[4])
            print("Rank Per Region: ",row[5])
            print("Overall Rank: ",row[6])
            print("Happiness Score:",row[7])
            print("Happiness Status: ",row[8])
            print("GDP per capita: ",row[9])
            print("Family:",row[10])
            print("Social support",row[11])
            print("Healthy life expectancy:",row[12])
            print("Freedom to make life choices: ",row[13])
            print("Generosity",row[14])
            print ("Perceptions of corruption: ",row[15], "\n")
    
    #Commit your changes in the database
    conn.commit()
    
    #Closing the connection
    conn.close()

def Json_output():
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
  
   
    conn.commit()

def visualisation():
    
        #establishing the connection
    conn = psycopg2.connect(
       database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
    )
    cursor = conn.cursor()
    

    plt.title('2015')
    # plt.xlabel('XXX axis title')
    # plt.ylabel('YYY axis title') 
    # #Show the plot
    # plt.show()
    
    engine = create_engine("postgresql:///?user=postgres&;password=postgres&database=Happiness&host=127.0.0.1&port=5432")
    
    df=pd.read_sql("select COUNTRY_DIMENSION_CODE As Country, happiness_score from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Switzerland','Iceland','Denmark','Norway') and calendar_date=20151231",engine)
    df1=pd.read_sql("select calendar_date, happiness_score, COUNTRY_DIMENSION_CODE as country from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Switzerland')",engine)
    df2=pd.read_sql("select substring(cast(calendar_date as varchar(10)),0,5) as year, happiness_score, COUNTRY_DIMENSION_CODE as country from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Denmark')",engine)
    
    plt.xlabel('country')
    plt.ylabel('happiness_score')
    width =0.3
    plt.bar(range(len(df['happiness_score'])), df['happiness_score']) 
    rects1 =plt.bar(df['country'],df['happiness_score']<2.6,color = ['red'],label=2015) 
    plt.bar(df['country'],df['happiness_score']>5.6,color = ['green'],label=2015)
    plt.show()
 
    print(df2['happiness_score'])
    plt.figure(num = 3, figsize=(8, 5))
    # plt.plot(df1['snapshot_date_dimension_id'],df1['happiness_score'])
    plt.title('Denmark')
    plt.plot(df2['year'],df2['happiness_score'])
    
    plt.legend()
    plt.tight_layout()
    plt.show()
    
def Api_insert():
    response =requests.get("http://api.worldbank.org/v2/country")
    # print(response.text)
    file_object = open('worldbank.xml', 'a')
    file_object.write(response.text)
    # Close the file
    file_object.close()
    
def read_xml():
     xml_data = open('worldbank.xml', 'r').read()  # Read file
     root = ET.XML(xml_data)  # Parse XML
    
     data = []
     cols = []
     for i, child in enumerate(root):
        data.append([subchild.text for subchild in child])
        cols.append(child.tag)
    
     df = pd.DataFrame(data).T  # Write in DF and transpose it
     df.columns = cols  # Update column names
     print(df)
     
if __name__ == "__main__":
    # insert_2015()
    # insert_2016()
    # insert_2017()
    # insert_json()
    # Modelling_record()
    # Json_output()
    # visualisation()
    # Api_insert()
    read_xml()
    