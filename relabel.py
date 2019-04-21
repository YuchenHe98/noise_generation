import sqlite3
from sqlite3 import Error

def create_connection(db_file):

    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

'''
This function is to detect the gratitude messages. You can add your own version
'''
def is_gratitude(text):
    return False


database = input("please input the database name: ")

# limit message can be something like "WHERE forumid = 'xxx'". Leave it blank if you want to relabel the whole db of threads.
limit_message = input("please input the limit message: ")

user_table = {}

conn = create_connection(database)
with conn:
    cur = conn.cursor() 
    cur.execute("SELECT id, user_title FROM user")
    rows = cur.fetchall()
    for each_user in rows:
        user_id = each_user[0]
        user_title = each_user[1]
        user_table[user_id] = user_title

conn = create_connection(database)
with conn:
    cur = conn.cursor()
    
    #drop_table_message = 'DROP TABLE augmentedThreads;'
    #cur.execute(drop_table_message)

    new_table_message = 'CREATE TABLE augmentedThreads (id TEXT PRIMARY KEY, inst_replied INT NOT NULL, intervention_time INT, intervention_message TEXT);'
    cur.execute(new_table_message)
    
    thread_message = 'SELECT id, starter FROM thread '
    thread_message += limit_message
    thread_message += ' ORDER BY posted_time'
    print(thread_message)
    cur.execute(thread_message)
    threads = cur.fetchall()
    for each_thread in threads:
        
        # All the info from this select operation
        thread_id = each_thread[0]
        thread_starter = each_thread[1]
        
        # Initialize intervention time as -1
        if(thread_starter in user_table and (user_table[thread_starter] == 'Instructor' or user_table[thread_starter] == 'Staff')):           
            continue

        # Detect the earliest intervention.            
        message_select = 'SELECT * FROM (SELECT id, user, post_text, post_time FROM post WHERE thread_id = \'%s\' UNION SELECT id, user, comment_text, post_time FROM comment WHERE thread_id = \'%s\') ORDER BY post_time;' % (thread_id, thread_id);
        cur.execute(message_select)
        # print(message_select)
        all_messages = cur.fetchall()

        intervened = False
        for each_message in all_messages:
            post_id = each_message[0]
            poster = each_message[1]
            post_text = each_message[2]
            post_time = each_message[3]

            if is_gratitude(post_text) or (poster in user_table and (user_table[poster] == 'Instructor' or user_table[poster] == 'Staff')):
                message_insert = 'INSERT INTO augmentedThreads VALUES(\'%s\', 1, %s, \'%s\')' % (thread_id, post_time, post_id)
                intervened = True
                break

        if not intervened:
            message_insert = 'INSERT INTO augmentedThreads (id, inst_replied) VALUES(\'%s\', 0)' % thread_id
            
        cur.execute(message_insert)
        