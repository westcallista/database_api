import sys
import logging
import rds_config
import pymysql
import os
import json
import asyncio
import requests
import time
from manychat_tools import *
#rds settings
rds_host  = "aws-simplified.cahq9cyseoxq.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = 3306

NO_CALL_RESPONSE = {'statusCode': 400, 'body': "Not a valid call. Check 'Call' key in payload."}

print(f"name {name}, password {password}, DB_NAME {db_name}")

#logger = logging.getLogger()
#logger.setLevel(logging.INFO)

connection = pymysql.connect(host=rds_host, user=name, passwd=password, db = db_name)
cursor = connection.cursor()

def lambda_handler(event, context):
    #cursor = connection.cursor()
    

    # Checking for body key in event otherwise body is event
    if "body" in event:
        data = json.loads(event['body'])
    else:
        data = event
    print("Body OBJ", data)
   
   
    
    # Checking for 'Call' key in request body. If no key, returns 400 response
    if "Call" in data:
        call = data["Call"]
        call = call.lower()
    else:
        return NO_CALL_RESPONSE

    # Checking for abbreviation in body and setting global mc_token variable
    if "Abbreviation" in data:
        company = data["Abbreviation"]
        global mc_token
        if "mc_token" in data:
            if "Bearer" in data["mc_token"]:
                mc_token = data["mc_token"]
            else:
                mc_token = "Bearer " + data["mc_token"]
        else:
            mc_token = get_mc_token_abv(company)
        print(f"COMPANY: {company}, MC_TOKEN: {mc_token}")

    #set_vcare_creds(data)
    
    user_id = valueOrDefault(data, "user_id")
    
    
    
    
    #print(valueOrDefault(data, "user_id"))
    update_row(data)
    read_and_print(data) 
    
 
# def insert(data):
#     #-----------INSERT-----------
#     insert = """insert into `Chat_log` (company, subscriber, current_flow, last_interaction, manychat_link, agent) values (%s, %s, %s, %s, %s, %s)
        
#     """
#     #cursor.execute(insert, ('Red Pocket', 'Todd Mitchell', '11 National Verifier', '2022-04-13 10:57:44', 'https://manychat.com/fb102678355731607/chat/7274655939242487', 'Jude'))
#     #cursor.commit()
    
#     #-----------SELECT-----------
#     #cursor.execute('SELECT * from Chat_log')
    
#     #-----------DELETE-----------
#     #cursor.execute('DELETE from Chat_log')
    
#     #-----------FIND-------------
# def find_row(data):
#     find = """select %s from `Chat_log` where user_id = %s """        ##### can't figure out why the parameter after 'where' wont work as %s
#     #find = """select '%s' from `Chat_log` where '%s' = '%s' """
#     #print(cursor.execute(find, ('company', 'Red Pocket')))
#     print(cursor.execute(find, ('user_id', '4950472321689032')))
#     #print(cursor.execute(find, ('agent', 'Jude')))
    
    
    #----------UPDATE--------------
def update_row(data):
    user_id_stored = valueOrDefault(data, "user_id")
    full_name_stored = valueOrDefault(data, "full_name")
    current_flow_stored = valueOrDefault(data, "00_CURRENT_FLOW")
    manychat_link_stored = valueOrDefault(data, "ChatURL")
    company_stored = valueOrDefault(data, "company")
    
    time_now = valueOrDefault(data, "last_interaction")
    
    
    find = """select %s from `Chat_log` where `user_id` = %s """        ##### can't figure out why the parameter after 'where' wont work as %s
    insert = """insert into `Chat_log` (company, subscriber, current_flow, last_interaction, manychat_link, agent, user_id) values (%s, %s, %s, %s, %s, %s, %s)"""
    print(cursor.execute(find, ('user_id', data["user_id"])))
    if cursor.execute(find, ('user_id', data["user_id"])) != 0:
        updateStatement = """UPDATE Chat_log SET subscriber = %s WHERE user_id = %s"""
        cursor.execute(updateStatement, (data["full_name"], data["user_id"]))
        updateStatement1 = "UPDATE Chat_log SET current_flow = %s WHERE user_id = %s"
        cursor.execute(updateStatement1, (data["00_CURRENT_FLOW"], data["user_id"]))
        updateStatement2 = "UPDATE Chat_log SET last_interaction = %s WHERE user_id = %s"
        cursor.execute(updateStatement2, (data["last_interaction"], data["user_id"]))
        updateStatement3 = "UPDATE Chat_log SET company = %s WHERE user_id = %s"
        cursor.execute(updateStatement3, (data["company"], data["user_id"]))
        connection.commit()
    # elif cursor.execute(find, ('manychat_link', data["ChatURL"])) != 0:
    #     updateStatement = "UPDATE Chat_log SET agent = 'Callie' WHERE user_id = 4950472321689032"
    #     cursor.execute(updateStatement)
    #     connection.commit()
    else:
        cursor.execute(insert, (data["company"], data["full_name"], data["00_CURRENT_FLOW"], data["last_interaction"], data["ChatURL"], 'Jude', data["user_id"]))
        connection.commit()
    #time.sleep(8)
    cursor.execute('SELECT * from Chat_log')
    rows = cursor.fetchall()
    
    # for row in rows:
    #     print("{0} {1} {2} {3} {4} {5} {6}".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    # #cursor.execute('SELECT * from Chat_log')
    

def read_and_print(data):
    #calling and printing all rows in the table
    cursor.execute('SELECT * from Chat_log')
    connection.commit()
    rows = cursor.fetchall()
    
    for row in rows:
      print("{0} {1} {2} {3} {4} {5} {6}".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        
    
# # Default to empty string if no value for key
def valueOrDefault(data, key, default=""):
    return data[key] if key in data else default    
