import mysql.connector as mysql
import dbConfig as guiConfig

import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from game_log_functions import GameLogSearch, EditGameLogExport, InsertIntoDatabase


# Open Database connection
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return [conn, curs]


# Close Database connection
def close_db_connection(conn):
    conn.close()


search_data = []
for attempt in range(10):
    try:
        # find all player tables in database
        table_list = []
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for (table,) in tables:
            if table == "prospects" or table == "teams" or table == "update_time":
                continue
            table_list.append(table)

        # for each player in table_list, go to database and find last row to get search information
        search_data = []
        for i in range(len(table_list)):
            cursor.execute("SELECT * FROM " + table_list[i] + " ORDER BY Date DESC LIMIT 1")
            data = cursor.fetchall()
            # for search purposes, need to add space between first and last name
            name = re.sub(r"(?<=\w)([A-Z])", r" \1", table_list[i])
            # if there is more than two spaces in name (ie Jean Luc Foudy), add hyphen in first space
            space_counter = len(re.findall(r"[ \n]+", name))
            if space_counter == 2:
                name = "-".join(name.split(" ", 1))
            # assemble search data
            search_data.append(name)
            for j in range(1, 4):
                search_data.append(data[0][j])

            # 1 - set league bit, search for game log
            call_game_log = GameLogSearch()
            league_bit = call_game_log.game_log_search(search_data)

            # 2 - edit game log
            edit_game_log = EditGameLogExport()
            if league_bit == 1:  # NCAA (CollegeHockeyInc)
                edit_game_log.colhockeyinc_game_logs(search_data)
            if league_bit == 2:  # USHL
                edit_game_log.ushl_game_log(search_data)
            if league_bit == 3:  # QMJHL
                edit_game_log.qmjhl_game_log(search_data)
            if league_bit == 4:  # OHL
                edit_game_log.ohl_game_log(search_data)

            # 3 - insert data into database function
            insert_game_log = InsertIntoDatabase()
            insert_game_log.insert_log(search_data, league_bit)

            # reset and move to next player
            search_data = []
        connection.close()

    except Exception as e:
        # if there is an error, log it...
        now = datetime.now()
        now_format = now.strftime("%m/%d/%Y %H:%M:%S")
        search_error = open(r'C:\NHLdb_pyqt\files\_search_error.txt', "a")
        search_error.write("ERROR on attempt " + str(attempt) + " at " + now_format +
                           " with the following data " + str(search_data) + ": " + str(e) + "\n")
        search_error.close()
        connection.close()
    else:
        break
else:
    now = datetime.now()
    now_format = now.strftime("%m/%d/%Y %H:%M:%S")
    search_error = open(r'C:\NHLdb_pyqt\files\_search_error.txt', "a")
    search_error.write("We failed 10 attempts ending at " + now_format + "\n")
    search_error.close()
    connection.close()
