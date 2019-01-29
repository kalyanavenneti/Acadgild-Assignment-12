
# coding: utf-8

# # Problem statement: 
# 
# Read the following data set:
# https://archive.ics.uci.edu/ml/machine-learning-databases/adult/
#     
# Rename the columns as per the description from this file:
# https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names

# In[1]:


import pandas as pd
import sqlite3


# In[7]:


# Read Data Set and Respective Columns

adlt_data_columns = pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names', sep=":")
print("Columns Name for DataSet")
adlt_data_columns.iloc[91:106,].index.tolist()


# In[14]:


# Reorder columns 
adlt_col_names= adlt_data_columns.iloc[91:106,].index.tolist()
adlt_col_names=adlt_col_names[1:]+adlt_col_names[0::-1]
adlt_col_names


# In[17]:


# Read Data Set annd Apply coumns Names

adlt_data=pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data' , names=adlt_col_names,header=None , index_col=False)

# Convert columns name to Title case
adlt_data.columns=adlt_data.columns.str.capitalize().tolist()
adlt_data.head(2)


# # Task
# 
# Create a sql db from adult dataset and name it sqladb

# In[18]:


# Create sqlab database using sqlite3 module 

connection = sqlite3.connect('sqladb.db') # Create Database
cursor = connection.cursor()


# In[22]:


# Create table 

connection.execute('''
    CREATE TABLE IF NOT EXISTS ADULTS (
         Age INTEGER,
         Workclass VARCHAR(20),
         Fnlwgt INTEGER,
         Education VARCHAR(20),
         Education_num INTEGER,         
         Marital_status VARCHAR(30),         
         Occupation VARCHAR(20),
         Relationship VARCHAR(20),
         Race VARCHAR(20),
         Sex VARCHAR(10),
         Capital_gain INTEGER,
         Capital_loss INTEGER,
         Hours_per_week INTEGER,
         Native_country VARCHAR(30),
         '>50k, <=50k.' VARCHAR(10)
        
    )
''')

connection.commit()


# In[23]:


# Insert Data into ADULTS table from  adlt_data dataset(dataframe)

insert_query = "INSERT INTO ADULTS (Age, Workclass, Fnlwgt, Education, Education_num, Marital_status, Occupation, Relationship, Race, Sex, Capital_gain, Capital_loss, Hours_per_week, Native_country, '>50k, <=50k.') values (%d,'%s', %d, '%s', %d, '%s','%s','%s','%s','%s',%d,%d,%d,'%s','%s')"

for index, row in adlt_data.iterrows():
    connection.execute(insert_query % (row['Age'], row['Workclass'], row['Fnlwgt'], row['Education'],row['Education-num'],row['Marital-status'],row['Occupation'],row['Relationship'],row['Race'],row['Sex'],row['Capital-gain'],row['Capital-loss'],row['Hours-per-week'],row['Native-country'],row['>50k, <=50k.']))

connection.commit()


# In[25]:


# Question 1. Select 10 records from the adult sqladb

conn = sqlite3.connect("sqladb.db") # connect to database 
df_Adults_10 = pd.read_sql_query("select * from ADULTS LIMIT 10;", conn) 

# Query the database and convert data into dataframe
print( "10 records from the adult sqladb:")
df_Adults_10


# In[26]:


# Create temporary variable for Sex and Workclass

Men , Workclass= ' Male', ' Private'

# Create query to assign sql object to data retrival 
query = "select  Sex , Workclass , AVG(Hours_per_week)  from ADULTS WHERE Sex='%s' and Workclass='%s'" % (Men, Workclass)
query


# In[27]:


# Question 2. Show me the average hours per week of all men who are working in private sector 
conn = sqlite3.connect("sqladb.db")   # connect to database 
df_Mens_Workclass = pd.read_sql_query(query, conn) # Query the database and convert data into dataframe
print("The average hours per week of all men who are working in private sector:")
df_Mens_Workclass


# In[31]:


# Question 3: Show me the frequency table for education, occupation and relationship, separately

query_education= "SELECT Education, count(Education) FROM ADULTS GROUP BY Education;"  # Query
conn = sqlite3.connect("sqladb.db") 
df_Eduction = pd.read_sql_query(query_education, conn) # Query the database and convert data into dataframe
print("Frequency table for Education ")
df_Eduction


# In[32]:


query_occupation = "SELECT Occupation, count(Occupation) FROM ADULTS GROUP BY Occupation;"
conn = sqlite3.connect("sqladb.db") 
df_occupation  = pd.read_sql_query(query_occupation, conn) # Query the database and convert data into dataframe
print("Frequency table for occupation  ")
df_occupation 


# In[33]:


query_relationship= "SELECT Relationship, count(Relationship) FROM ADULTS GROUP BY Relationship;"
conn = sqlite3.connect("sqladb.db") 
df_relationship  = pd.read_sql_query(query_relationship, conn) # Query the database and convert data into dataframe
print("Frequency table for relationship  ")
df_relationship


# In[36]:


# Question 4: Are there any people who are married, working in private sector and having a masters degree

# Create temporary variable for Marital Status, Workclass information , and Education Status
Workclass, Education , Married= ' Private', ' Masters', ' Married%'

# Create query
query_people= "select  Workclass , Education , Marital_status , COUNT(*)  from ADULTS WHERE Workclass='%s' and Education='%s' and Marital_status LIKE '%s' GROUP BY Workclass , Education , Marital_status" % (Workclass, Education,Married)

conn = sqlite3.connect("sqladb.db") 
df_people  = pd.read_sql_query(query_people, conn) # Query the database and convert data into dataframe

df_people


# In[38]:


# Alter  Dataframe to get data for people who are married 
df_people.replace([df_people['Marital_status'][0],df_people['Marital_status'][1]],value="Married", inplace=True)
print("People who are married, working in private sector and having a masters degree")
df_people.groupby(['Workclass','Education','Marital_status'],as_index=False).sum()


# In[40]:


# Question 5: What is the average, minimum and maximum age group for people working in different sectors

query_Workclass= "SELECT Workclass, AVG(Age) , MIN(Age), MAX(Age) FROM ADULTS GROUP BY Workclass ;"
conn = sqlite3.connect("sqladb.db") 
df_Workclass  = pd.read_sql_query(query_Workclass, conn) # Query the database and convert data into dataframe
print("Average, minimum and maximum age group for people working in different sectors ")
df_Workclass


# In[42]:


# Question 6: Calculate age distribution by country

query_age= "SELECT Native_country, Age , COUNT(Age) FROM ADULTS GROUP BY Native_country, Age ;"
conn = sqlite3.connect("sqladb.db")
df_Country_age  = pd.read_sql_query(query_age, conn) # Query the database and convert data into dataframe
df_Country_age.head()


# In[43]:


# Alter dataframe and replace unwanted data with NA

df_Country_age = df_Country_age.apply(lambda x: x.str.strip() if x.dtype == "object" else x) # remove white space in Country column
df_Country_age['Native_country'].unique()


# In[44]:


df_Country_age.replace('?',value='NA',inplace=True) # Replace '?' with NA

# Sort dataframe via Country and age 
df_Country_age.sort_values(['Native_country','Age'], axis=0, ascending=[True,True] , inplace=True ) 

df_Country_age.reset_index(drop=True , inplace=True)

print("age distribution by country  ")
df_Country_age.head(20)


# In[45]:


# Question 7: Compute a new column as 'Net-Capital-Gain' from the two columns 'capital-gain' and 'capital-loss'

query_Capital="SELECT * FROM ADULTS;"
conn = sqlite3.connect("sqladb.db") 
df_Capital  = pd.read_sql_query(query_Capital, conn) # Query the database and convert data into dataframe
df_Capital.head()


# In[46]:


# Create New column 'Net-Capital-Gain in  Dataframe from column Capital_gain
df_Capital['Net-Capital-Gain']=df_Capital['Capital_gain']+df_Capital['Capital_loss'] 
df_Capital.head()


# In[47]:


# Get a list of the columns
col_list = list(df_Capital.columns)
# swap column orders
col_list[12], col_list[13] ,  col_list[14],col_list[15] = col_list[15], col_list[12], col_list[13] ,  col_list[14]
col_list


# In[48]:


df_Capital=df_Capital.loc[:,col_list ] # Reframe dataframe

print("Net Capital gain")
df_Capital.head()

