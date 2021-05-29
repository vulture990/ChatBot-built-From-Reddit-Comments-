import json
import sqlite3
from datetime import datetime
import time 

query_list=[]
sql_transaction=[]
conncetion=sqlite3.connect('{}.db'.format("database"))
cursor=conncetion.cursor()
def create_table():
    cursor.execute("""CREATE TABLE IF NOT EXISTS parent_reply 
    (parent_id TEXT PRIMARY KEY,comment_id TEXT UNIQUE ,parent TEXT,comment TEXT ,subreddit TEXT,unix_time INT,score INT)""")


def format_data(data):
    #doing some cleaning up 
    data=data.replace("\n","newlinechar").replace("\r","newlinechar").replace('"',"'")
    return data



def find_parent(id):
    try:
        sql="SELECT commment FROM parent_reply WHERE comment_id= '{}' LIMIT 1".format(id)
        cursor.execute(sql)
        result=cursor.fetchone()
        if result!=None:
            return result[0]
        else:
            return False
    except Exception as e:
        #print("find_parent",e)
        return False


def find_existing_score(id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id= '{}' LIMIT 1".format(id)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    
    except Exception as e:
        print("find_parent",e)
        return False


def acceptable(data):
  if len(data.split(" ")) > 50 or len(data.split(" ")) < 1:
    return False
  elif len(data) > 1000:
    return False
  elif data == '[deleted]' or data == '[removed]':
    return False
  else:
    return True


def add_query(query):
  global query_list
  query_list.append(query)
  if len(query_list) > 1000:
    cursor.execute("begin transaction")
    for query in query_list:
      try:
        cursor.execute(query)
      except Exception as e:
        print('error in ' + query)
    conncetion.commit()
    query_list = []



    
if __name__=="__main__":
    create_table()
    row_count=0
    paired_rows=0

    with open("G:/chatbot/RC_2017-11",buffering=1000) as f:
        for row in f:
            print(row)
            
            row_count+=1
            row=json.loads(row)
            parent_id=row['parent_id']
            body=format_data(row['body'])
            create_utc=row['created_utc']
            score=row['score']
            subreddit=row['subreddit']
            parent_data=find_parent(parent_id)



            if score >0:
                if acceptable(body):
                    existing_comment=find_existing_score(parent_id)
                    if existing_comment:
                        if score>existing_comment:
                  









