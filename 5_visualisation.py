# -*- coding: utf-8 -*-
"""
Created on Fri May  7 09:31:16 2021

@author: parjag
"""


import psycopg2
import csv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import numpy as np

#establishing the connection
conn = psycopg2.connect(
   database="Happiness", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
)
cursor = conn.cursor()


# create a figure and axis 
# fig, ax = plt.subplots() 
# # count the occurrence of each class 
# data = wine_reviews['points'].value_counts() 
# # get x and y data 
# points = data.index 
# frequency = data.values 
# # create bar chart 
# ax.bar(points, frequency) 
# # set title and labels 
# ax.set_title('Wine Review Scores') 
# ax.set_xlabel('Points') 
# ax.set_ylabel('Frequency')


#Creating the dataset
# df = sns.load_dataset('titanic') 
# df=df.groupby('who')['fare'].sum().to_frame().reset_index()
# #Creating the column plot 
# plt.bar(df['who'],df['fare'],color = ['#F0F8FF','#E6E6FA','#B0E0E6']) 
# #Adding the aesthetics
plt.title('2015')
# plt.xlabel('XXX axis title')
# plt.ylabel('YYY axis title') 
# #Show the plot
# plt.show()

engine = create_engine("postgresql:///?user=postgres&;password=postgres&database=Happiness&host=127.0.0.1&port=5432")

df=pd.read_sql("select COUNTRY_DIMENSION_CODE As Country, happiness_score from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Switzerland','Iceland','Denmark','Norway') and calendar_date=20151231",engine)
df1=pd.read_sql("select calendar_date, happiness_score, COUNTRY_DIMENSION_CODE as country from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Switzerland')",engine)
df2=pd.read_sql("select substring(cast(calendar_date as varchar(10)),0,5) as year, happiness_score, COUNTRY_DIMENSION_CODE as country from WH.COUNTRY_HAPPINESS_FACT where COUNTRY_DIMENSION_CODE IN ('Denmark')",engine)
# print (df)
# #Adding the aesthetics
#plt.title('Happiness')
# plt.xlabel('Happiness Score')
# plt.ylabel('Year') 

#df.plot(kind="bar",x="country", y="happiness_score")
# print(df['happiness_score'])
# if (df['happiness_score']) > 5.6:
plt.xlabel('country')
plt.ylabel('happiness_score')
width =0.3
plt.bar(range(len(df['happiness_score'])), df['happiness_score']) 
rects1 =plt.bar(df['country'],df['happiness_score']<2.6,color = ['red'],label=2015) 
plt.bar(df['country'],df['happiness_score']>5.6,color = ['green'],label=2015)
plt.show()
# plt.bar(range(len(df1['happiness_score'])), df1['happiness_score']) 
# rects2 =plt.bar(df1['country'],df1['happiness_score']<2.6,color = ['red'],label=2016) 
# plt.bar(df1['country'],df1['happiness_score']>5.6,color = ['green'],label=2016)
# plt.bar(np.arange(len(df['country'])), df['happiness_score'], width=width)
# plt.bar(df['country'], 5.6 < df['happiness_score'] & df['happiness_score'] < 2.6,color = ['yellow'])
# clrs = ['red' if (df['happiness_score'] <2.6) else 'green' for x in df['happiness_score']]
# plt.bar(df['country'],df['happiness_score'],color=clrs)
# plt.xticks(np.arange(4), ('G1', 'G2', 'G3', 'G4'))
# plt.legend()
print(df2['happiness_score'])
plt.figure(num = 3, figsize=(8, 5))
# plt.plot(df1['snapshot_date_dimension_id'],df1['happiness_score'])
plt.title('Denmark')
plt.plot(df2['year'],df2['happiness_score'])

plt.legend()
plt.tight_layout()
plt.show()

