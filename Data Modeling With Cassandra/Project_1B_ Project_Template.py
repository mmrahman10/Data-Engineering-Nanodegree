#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[9]:


# Drop Table if Exist
delete_sessions_table = "DROP TABLE IF EXISTS sessions"
session.execute(delete_sessions_table)

create_sessions_table = "CREATE TABLE IF NOT EXISTS sessions (artist text, item_in_session int, length float, session_id int, song_title text, PRIMARY KEY (session_id, item_in_session))"
session.execute(create_sessions_table)

#### Creating the table to insert event data from the Directory of .csv file
# In[1]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
                    


# In[16]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO sessions (artist, item_in_session, length, session_id, song_title)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (ascii(line[0]), int(line[3]), float(line[5]), int(line[8]), ascii(line[9])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[17]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query1 = "select artist, song_title, length from sessions WHERE session_id = 338 and item_in_session = 4"
rows = session.execute(query1)
for row in rows:
    print (row.artist, row.song_title, row.length)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[18]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

#Dropping the table user if exist for query2
delete_users_table = "DROP TABLE IF EXISTS users"
session.execute(delete_users_table)

#Creating the table user for query2
create_users_table = "CREATE TABLE IF NOT EXISTS users (artist text, first_name text, item_in_session int, last_name text, session_id int, song_title text, user_id int, PRIMARY KEY ((user_id, session_id), item_in_session))"
session.execute(create_users_table)                    


# In[19]:


#Insert all data into the users table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO users (artist, first_name, item_in_session, last_name, session_id, song_title, user_id)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (line[0], line[1], int(line[3]), line[4], int(line[8]), line[9], int(line[10]))) 


# In[20]:


query2 = "select artist, song_title, first_name, last_name from users WHERE session_id = 182 and user_id = 10"
rows = session.execute(query2)
for row in rows:
    print (row.artist, row.song_title, row.first_name, row.last_name)


# In[21]:


#Create the song_listens tablefor query 3
drop_song_listens = "DROP TABLE IF EXISTS song_listens"
session.execute(drop_song_listens)

create_song_listens = "CREATE TABLE IF NOT EXISTS song_listens (first_name text, last_name text, song_title text, user_id int, PRIMARY KEY (song_title, user_id))"
session.execute(create_song_listens)


# In[22]:


#Insert all data into the song_listens table
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_listens (first_name, last_name, song_title, user_id)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[1], line[4], line[9], int(line[10]))) 


# In[23]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
query3 = "select first_name, last_name from song_listens WHERE song_title = 'All Hands Against His Own'"
rows = session.execute(query3)
for row in rows:
    print (row.first_name, row.last_name)
                   


# In[ ]:





# In[ ]:





# ### Drop the tables before closing out the sessions

# In[24]:


## TO-DO: Drop the table before closing out the sessions
# Drop Table sessions
delete_sessions_table = "DROP TABLE IF EXISTS sessions"
session.execute(delete_sessions_table)
#Dropping the table user if exist for query2
delete_users_table = "DROP TABLE IF EXISTS users"
session.execute(delete_users_table)
# Drop Table song_listens
drop_song_listens = "DROP TABLE IF EXISTS song_listens"
session.execute(drop_song_listens)


# In[ ]:





# ### Close the session and cluster connection¶

# In[25]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




