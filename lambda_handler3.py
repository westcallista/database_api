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
    
    call_mapping = {
        "update_row": update_row,
        "read_and_print": read_and_print,
        "add_column": add_column,
        "find": find
    }
    try:
        res = call_mapping[call](data)
        # print("FINAL MAIN RESPONSE", json.dumps(res))
        return {"statusCode": 200, "body": json.dumps(res)}
    except KeyError:
        return NO_CALL_RESPONSE
    
    
    
    
    #print(valueOrDefault(data, "user_id"))
    #update_row(data)
    #read_and_print(data) 
    #delete_column("Assistance")
    #add_column("Assistance_Plan")
    #show_columns()
    
    #----------ADD NEW COLUMN--------------    
def add_column(new_column):
    #add_string = "ALTER TABLE Chat_log ADD %s VARCHAR(255) "
    #cursor.execute(add_string, new_column)
    add_string = "ALTER TABLE Chat_log ADD {} VARCHAR(255)".format(new_column)
    cursor.execute(add_string)
    
    
def show_columns():
    show = """SHOW COLUMNS FROM Chat_log"""
    cursor.execute(show)
    
    
    #----------UPDATE--------------
def update_row(data):
    user_id_stored = valueOrDefault(data, "user_id")
    full_name_stored = valueOrDefault(data, "full_name")
    current_flow_stored = valueOrDefault(data, "00_CURRENT_FLOW")
    manychat_link_stored = valueOrDefault(data, "ChatURL")
    company_stored = valueOrDefault(data, "company")
    
    time_now = valueOrDefault(data, "last_interaction")
    
    
    find = """select %s from `Chat_log` where `user_id` = %s """        ##### can't figure out why the parameter after 'where' wont work as %s
    insert = """insert into `Chat_log` (company, subscriber, current_flow, last_interaction, manychat_link, agent, user_id, Assistance_Plan) values (%s, %s, %s, %s, %s, %s, %s, %s)"""
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
        updateStatement3 = "UPDATE Chat_log SET Assistance_Plan = %s WHERE user_id = %s"
        cursor.execute(updateStatement3, (data["Assistance_Plan"], data["user_id"]))
        connection.commit()
    else:
        cursor.execute(insert, (data["company"], data["full_name"], data["00_CURRENT_FLOW"], data["last_interaction"], data["ChatURL"], data["agent"], data["user_id"], data["Assistance_Plan"]))
        connection.commit()
    cursor.execute('SELECT * from Chat_log')
    rows = cursor.fetchall()
    

def find(data):
    finding = """select * from `Chat_log` where %s = %s """
    finding_company = """select * from `Chat_log` where company = %s """
    finding_userId = """select * from `Chat_log` where user_id = %s """
    finding_fullName = """select * from `Chat_log` where full_name = %s """
    finding_flow = """select * from `Chat_log` where current_flow = %s """
    finding_url = """select * from `Chat_log` where manychat_link = %s """
    finding_lastInteraction = """select * from `Chat_log` where last_interaction = %s """
    finding_agent = """select * from `Chat_log` where agent = %s """
    finding_plan = """select * from `Chat_log` where Assistance_Plan = %s """
    if data["key"] == "company":
        print(cursor.execute(finding_company, (data["value"])))
    if data["key"] == "user_id":
        print(cursor.execute(finding_userId, (data["value"])))
    elif data["key"] == "full_name":
        print(cursor.execute(finding_fullName, (data["value"])))
    elif data["key"] == "current_flow":
        print(cursor.execute(finding_flow, (data["value"])))
    elif data["key"] == "manychat_link":
        print(cursor.execute(finding_url, (data["value"])))
    elif data["key"] == "last_interaction":
        print(cursor.execute(finding_lastInteraction, (data["value"])))
    elif data["key"] == "agent":
        print(cursor.execute(finding_agent, (data["value"])))
    elif data["key"] == "Assistance_Plan":
        print(cursor.execute(finding_plan, (data["value"])))
    else:
        return
    connection.commit()
    rows = cursor.fetchall()
    
    for row in rows:
      print("{0} {1} {2} {3} {4} {5} {6} {7}".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
    
    #----------PRINT DATA IN TABLE--------------
def read_and_print(data):
    #calling and printing all rows in the table
    cursor.execute('SELECT * from Chat_log')
    connection.commit()
    rows = cursor.fetchall()
    
    for row in rows:
      print("{0} {1} {2} {3} {4} {5} {6} {7}".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
        
    
# # Default to empty string if no value for key
def valueOrDefault(data, key, default=""):
    return data[key] if key in data else default 

    #----------DELETE AN UNWANTED COLUMN FROM THE TABLE--------------   
def delete_column(unwanted_col):
    delete = f"ALTER TABLE Chat_log DROP COLUMN {unwanted_col}"
    cursor.execute(delete)


    
