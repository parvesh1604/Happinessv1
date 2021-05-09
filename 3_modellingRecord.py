# -*- coding: utf-8 -*-
"""
Created on Sat May  8 14:58:46 2021

@author: parjag
"""

import psycopg2

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