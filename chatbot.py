import sqlite3
import json  # used to load the lines from data
from datetime import datetime  # used to log



sql_transaction = []
#if the database doesn't exist, sqlite3 will create the database

connection = sqlite3.connect('{}.db'.format("database2"))
c = connection.cursor()


def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")


def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace(
        '\r', ' newlinechar ').replace('"', "'")
    return data


def transaction_bldr(sql):
    global sql_transaction
    # keep appending the sql statements to the transaction until it's a certain size
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        # for each sql statement we will try to execute it, otherwise we will just
        # accept the statement
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        # after we execute all the statements, we will just commit
        connection.commit()
        # and then empty out the transaction
        sql_transaction = []


def acceptable(data):
    # since we'll be using multiple models, we need to keep the data at 50 words
    # we need to make sure that the data has at least 1 word and isn't an empty comment
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    # we don't want to use data with more than 1,000 characters
    elif len(data) > 1000:
        return False
    # we don't want to use comments that are just [deleted] or [removed]
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True


def find_parent(pid):
    try:
        # looks for anywhere where the comment_id is the parent
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(
            pid)
        # execute and return results
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        #print(str(e))
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(
            pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        #print(str(e))
        return False


def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        # overwrite the information with a new comment with better score
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        # inserting a new row with parent id and parent body
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        # insert information in case the comment is a parent for another comment
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(
            parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion', str(e))


if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0

    with open("G:/chatbot/RC_2017-11", buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            # using a helper function called format_data to clean up the data
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['link_id']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)
            if score >0:
                existing_comment_score = find_existing_score(parent_id)
                if existing_comment_score:
                    if score > existing_comment_score:
                        if acceptable(body):
                            sql_insert_replace_comment(
                                comment_id, parent_id, parent_data, body, subreddit, created_utc, score)

                else:
                    if acceptable(body):
                        if parent_data:
                            sql_insert_has_parent(
                                comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(
                                comment_id, parent_id, body, subreddit, created_utc, score)
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(
                    row_counter, paired_rows, str(datetime.now())))
