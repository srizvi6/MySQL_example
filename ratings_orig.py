from kafka import KafkaConsumer
import os
import pandas as pd
import re
import time
import mysql.connector
from statistics import mean
#this is for debugging locally. This should remain commented when the script is running in prod
# os.system("ssh -o ServerAliveInterval=60 -L 9092:localhost:9092 tunnel@128.2.204.215 -NTf")



def db_insert(db, cursor, input_log):
    """This function parses through the log in the timeframe set below in the main function and gets the user ratings and stores the userid, movie and rating into the raitngsdb database. 
    """
    condition= re.compile("[a-zA-Z ]*/rate/[a-zA-Z ]*")
 
    for i in range(0,len(input_log)):
        #search the log for ratings
        if(condition.search(input_log.loc[i,"entries"])):
            rec=input_log.loc[i,"entries"]

            #parse the entry to get the user id, movie and rating   
            rec_sections= rec.split(",")
            movie = rec_sections[2].split("/")[2].strip()
            rating = movie.split("=")[1].strip()
            movie =movie[:-2]
            userid = rec_sections[1]

            values = (userid, movie, rating)
            sql_query = "INSERT INTO user_ratings (userID, movie, rating) VALUES (%s, %s, %s)"

            cursor.execute(sql_query, values)
            db.commit()

def get_unique_ids(cursor):
    "Function to get unique user id's in case a user rated multiple movies,"
    cursor.execute("SELECT DISTINCT userID FROM user_ratings")
    user_ids = cursor.fetchall()
    return user_ids

def retrieve_and_calculate(cursor, unique_ids):
    "Simple function to retrive the movies that a user has rated from the database given the user's id"
    happy_users = 0
    for i in range(0, len(unique_ids)):
        cursor.execute(f"SELECT * FROM user_ratings WHERE userID LIKE {unique_ids[i][-1]}")
        entries = cursor.fetchall()
        
        sum = 0
        for entry in entries:
            if entry[2] == 'E':
                continue
            sum += int(entry[2])
        average = (sum/len(entries))
        if average > 2:
            happy_users += 1
    print(happy_users, "total users have given positive ratings.")
    os.system(f"echo At {(time.strftime('%l:%M%p:%b%d,%Y'))[1:]}, {happy_users} total users have given positive ratings. >> ratings_log.txt")

def main():
    mydb = mysql.connector.connect(host='localhost',database='ratingsdb',user='root',password='Alloy123')

    mycursor = mydb.cursor()
    
    # Create a consumer to read data from kafka
    topic = 'movielog12'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        # Read from the start of the topic; Default is latest
        auto_offset_reset='latest',
        # Commit that an offset has been read
        enable_auto_commit=True,
    )
    
    print('Reading Kafka Broker for movielog12')

    log_store = f"kafka_log_{(time.strftime('%l:%M%p:%b%d,%Y'))[1:]}.csv"
    current_time = time.time()

    #This is the main code that will be running
    for message in consumer:
        message = message.value.decode('utf-8')
        os.system(f"echo {message} >> {log_store}")
        
        # record the number of ratings collected every 5s
        if time.time() - current_time >= 10:
            input_log= pd.read_csv(log_store, sep = '\t')
            input_log.columns = ["entries"]
            db_insert(mydb, mycursor, input_log)

            unique_ids = get_unique_ids(mycursor)

            retrieve_and_calculate(mycursor, unique_ids)

            #delete the old log to avoid using up space. Can be commented for debugging
            os.system(f"rm {log_store}")

            #write to a new log
            log_store = f"kafka_log_{(time.strftime('%l:%M%p:%b%d,%Y'))[1:]}.csv"
            current_time = time.time()

if __name__ == "__main__":
    
    main()