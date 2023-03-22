# Importing libraries
import sqlite3
import urllib.request
import ssl
import json
import time

# Creating sqlite3 connection
connection = sqlite3.connect('dsc450.db')

# Creating a cursor
cursor = connection.cursor()

# Stores drop table statements
drop_table_statement_1 = """DROP TABLE User"""

# Executes drop table statements so that a new table with same name can be created
cursor.execute(drop_table_statement_1)

# Creating User table
# Storing query
create_table_statement_1 = """
CREATE TABLE User
(

    ID VARCHAR(30),
    Name VARCHAR(20),
    ScreenName VARCHAR(20),
    Description VARCHAR(20),
    FriendsCount NUMBER(9),
    
        PRIMARY KEY (ID)

)
"""

# Executing query
cursor.execute(create_table_statement_1)

# Creating Tweet table
# Stores drop table statements
drop_table_statement_2 = """DROP TABLE Tweet"""

# Executes drop table statements so that a new table with same name can be created
cursor.execute(drop_table_statement_2)

# Stores create table statements
create_table_statement_2 = """
CREATE TABLE IF NOT EXISTS Tweet
(
    UserID VARCHAR(30),
    GeoID VARCHAR(30),
    CreatedAt DATE,
    IDStr VARCHAR2(25),
    Text VARCHAR2(280),
    Source VARCHAR2(200),
    InReplyToUserID VARCHAR2(280),
    InReplyToScreenName VARCHAR2(280),
    InReplyToStatusID VARCHAR2(280),
    ReTweetCount NUMBER(5),
    Contributors VARCHAR2(30),
  
        FOREIGN KEY (UserID)
            REFERENCES User (ID),
            
        FOREIGN KEY (GeoID)
            REFERENCES Geo (ID)
  
)
"""

# Creates table
cursor.execute(create_table_statement_2)

# Creating Geo table
# Stores drop table statements
drop_table_statement_3 = """DROP TABLE Geo"""

# Executes drop table statements so that a new table with same name can be created
cursor.execute(drop_table_statement_3)

# Stores create table statements
create_table_statement_3 = """
CREATE TABLE IF NOT EXISTS Geo
(
    ID VARCHAR(30),
    Type VARCHAR(30),
    Longitude VARCHAR2(50),
    Latitude VARCHAR2(50),
    
        PRIMARY KEY (ID)
  
)
"""

# Creates table
cursor.execute(create_table_statement_3)

# Avoiding urllib.error.URLError: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed:
# certificate has expired (_ssl.c:1108)>
ssl._create_default_https_context = ssl._create_unverified_context

# Acessing text file
web = urllib.request.urlopen('https://dbgroup.cdm.depaul.edu/DSC450/Module7.txt')

# Storing it
web_lines = web.readlines()

# Creates lists to store tweets and problematic tweets
tweets = []
problematic_tweets = []

# Opening a file
file = open('Error.txt', 'w')

# Iterates over lines of tweets
for line in web_lines:

    # Tries to load it
    try:

        # Storing it in a json dictionary
        tweets.append(json.loads(line))

    # If error occurs
    except ValueError:

        # Storing problematic tweets
        file.write(str(line))

# Closing file
file.close()

# Creates empty lists
temp_tweet_list = []
tweet_list = []
temp_tweet_user_list = []
tweet_user_list = []
temp_tweet_geo_list = []
tweet_geo_list = []

tweet_geo_list

# Iterates over lines of tweet
for tweet in tweets:

    # Appends values from dictionary to a temporary list
    temp_tweet_list.append(tweet['id'])
    
    # If tweet is geo enabled
    if tweet['place'] is not None and tweet['geo'] is not None:
        
        # Append it
        temp_tweet_list.append(tweet['place']['id'])
        temp_tweet_geo_list.append(tweet['place']['id'])
        temp_tweet_geo_list.append(tweet['geo']['type'])
        temp_tweet_geo_list.append(tweet['geo']['coordinates'][0])
        temp_tweet_geo_list.append(tweet['geo']['coordinates'][1])
        
        # Appends temporary list to list
        tweet_geo_list.append(temp_tweet_geo_list)
        
    # Otherwise
    else:
        
        # Appends None
        temp_tweet_list.append(None)
        
    temp_tweet_list.append(tweet['created_at'])
    temp_tweet_list.append(tweet['id_str'])
    temp_tweet_list.append(tweet['text'])
    temp_tweet_list.append(tweet['source'])
    temp_tweet_list.append(tweet['in_reply_to_user_id'])
    temp_tweet_list.append(tweet['in_reply_to_screen_name'])
    temp_tweet_list.append(tweet['in_reply_to_status_id'])
    temp_tweet_list.append(tweet['retweet_count'])
    temp_tweet_list.append(tweet['contributors'])
    
    temp_tweet_user_list.append(tweet['id'])
    temp_tweet_user_list.append(tweet['user']['name'])
    temp_tweet_user_list.append(tweet['user']['screen_name'])
    temp_tweet_user_list.append(tweet['user']['description'])
    temp_tweet_user_list.append(tweet['user']['friends_count'])

    # Appends temporary list to list
    tweet_list.append(temp_tweet_list)
    tweet_user_list.append(temp_tweet_user_list)

    # Clears temporary list
    temp_tweet_list = []
    temp_tweet_user_list = []
    temp_tweet_geo_list = []

# Iterates over lines of list
for line in tweet_user_list:

    # Executes insert statement
    cursor.execute("INSERT OR IGNORE INTO User VALUES (?, ?, ?, ?, ?);", line)

# Iterates over lines of list
for line in tweet_list:

    # Executes insert statement
    cursor.execute("INSERT OR IGNORE INTO Tweet VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", line)

# Iterates over lines of list
for line in tweet_geo_list:

    # Executes insert statement
    cursor.execute("INSERT OR IGNORE INTO Geo VALUES (?, ?, ?, ?);", line)

# Comparing SQL and python's performance
# SQL
# Storing start time
start_time = time.time()

# Creating and storing SQL statement
statement_4 = """
SELECT IDStr
FROM Tweet
WHERE IDStr LIKE '%789%' OR IDStr LIKE '%987%'
"""

# Using cursor to execute statement
results_4 = cursor.execute(statement_4)

# Storing results
table_4 = results_4.fetchall()

# Printing results
print(table_4)

# Storing end time
end_time = time.time()

# Calculating execution time
execution_time = end_time - start_time

# Printing execution time
print(f'Execution time: {execution_time}')

# Python
# Storing start time
start_time = time.time()

# For every tweet in tweet_list
for tweet in tweet_list:
    
    # If 789 or 987 are in the IDString column
    if '789' or '987' in tweet[3]:
        
        # Print tweet
        print(tweet[3])
        
# Storing end time
end_time = time.time()

# Calculating execution time
execution_time = end_time - start_time

# Printing execution time
print(f'Execution time: {execution_time}')

# SQL
# Storing start time
start_time = time.time()

# Creating and storing SQL statement
statement_5 = """
SELECT COUNT(DISTINCT(FriendsCount))
FROM User
"""

# Using cursor to execute statement
results_5 = cursor.execute(statement_5)

# Storing results
table_5 = results_5.fetchall()

# Printing results
print(f'Unique values on FriendsCount column: {table_5[0][0]}')

# Storing end time
end_time = time.time()

# Calculating execution time
execution_time = end_time - start_time

# Printing execution time
print(f'Execution time: {execution_time}')

# Python
# Storing start time
start_time = time.time()

# Creating a list to hold unique values
unique_list = []

# For every user in the tweet_user_list
for user in tweet_user_list:
    
    # If value from FriendsCount column is not in the unique_list
    if user[4] not in unique_list:
        
        # Add value to unique_list
        unique_list.append(user[4])
        
# Storing number of unique values
unique_fl_values = len(unique_list)

print(f'Unique values on FriendsCount column: {unique_fl_values}')
        
# Storing end time
end_time = time.time()

# Calculating execution time
execution_time = end_time - start_time

# Printing execution time
print(f'Execution time: {execution_time}')

# Commit
connection.commit()

# Closing connection
connection.close()










# =============================================================================
# # Using cursor to execute select statement and storing results
# results_1 = cursor.execute('SELECT * FROM User')
# 
# # Passing results to fetchall function and storing it
# table_1 = results_1.fetchall()
# 
# # Creating a counter
# counter = 0
# 
# print('User table:')
# 
# # Looking at a few table lines
# for line in table_1:
#     counter += 1
#     print(line)
# 
#     if counter == 30:
#         break
# 
# # Printing an empty space in between tables
# print()
# 
# # Using cursor to execute select statement and storing results
# results_2 = cursor.execute('SELECT * FROM Tweet')
# 
# # Passing results to fetchall function and storing it
# table_2 = results_2.fetchall()
# 
# # Creating a counter
# counter = 0
# 
# print('Tweet table:')
# 
# # Looking at a few table lines
# for line in table_2:
#     counter += 1
#     print(line)
# 
#     if counter == 30:
#         break
# 
# # Printing an empty space in between tables
# print()
# 
# # Using cursor to execute select statement and storing results
# results_3 = cursor.execute('SELECT * FROM Geo')
# 
# # Passing results to fetchall function and storing it
# table_3 = results_3.fetchall()
# 
# # Creating a counter
# counter = 0
# 
# print('Geo table:')
# 
# # Looking at a few table lines
# for line in table_3:
#     counter += 1
#     print(line)
# 
#     if counter == 30:
#         break
# =============================================================================

