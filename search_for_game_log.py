from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import pandas as pd
import numpy
import time
from datetime import datetime
import mysql.connector as mysql
import dbConfig as guiConfig


# Open Database connection
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return[conn, curs]


# Close Database connection
def close_db_connection(conn):
    conn.close()


class GameLogSearch:
    def game_log_search(self, search_text):
        # function used for data frame
        def row_get_data_text(table_row, col_tag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in table_row.find_all(col_tag)]
        # Start
        start = time.time()
        # different teams will have different google searches
        league_bit = 0
        if search_text[2] == "Boston College":
            # get season
            year = search_text[1][-2:]
            webpage = "https://collegehockeyinc.com/teams/boston-college/roster" + year + ".php"
            # set league bit
            league_bit = 1
        elif search_text[2] == "Univ. of Notre Dame":
            # get season
            year = search_text[1][-2:]
            webpage = "https://collegehockeyinc.com/teams/notre-dame/roster" + year + ".php"
            # set league bit
            league_bit = 1
        elif search_text[2] == "Univ. of Minnesota":
            # get season
            year = search_text[1][-2:]
            webpage = "https://collegehockeyinc.com/teams/minnesota/roster" + year + ".php"
            # set league bit
            league_bit = 1
        else:
            print("Search not yet available...DO IT TO IT LARS....I mean Greg")
            return
        options = Options()
        options.page_load_strategy = 'eager'
        options.add_argument('--headless')
        options.add_argument('--disable gpu')
        driver = webdriver.Chrome(executable_path=r"C:\chromedriver.exe", options=options)
        driver.get(webpage)
        # go to stats page for player
        if league_bit == 1:  # NCAA
            # navigate to stats table
            link = driver.find_element_by_partial_link_text(search_text[0])
            link.click()
            # find stats table
            table = "base"
            data = []
            soup = BeautifulSoup(driver.page_source, "html.parser")
            stats_table = soup.find("table", {"class": table})
            if stats_table is None:
                # if table not found, try again
                soup = BeautifulSoup(driver.page_source, "html.parser")
                stats_table = soup.find("table", {"class": table})
            if stats_table is not None:
                # table found, start getting data
                trs = stats_table.find_all('tr')
                header_row = row_get_data_text(trs[0], 'th')
                if header_row:  # if there is a header row include first
                    data.append(header_row)
                    trs = trs[1:]
                for tr in trs:  # for every table row
                    data.append(row_get_data_text(tr, 'td'))  # data row
                # make data frame, save to .csv
                data_table_df = pd.DataFrame(data)
                data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + ' ' + search_text[1] + '.xlsx')
                # find run time to get data
                end = time.time()
                search_time = end - start
                now = datetime.now()
                now_format = now.strftime("%m/%d/%Y %H:%M:%S")
                time_for_search = open(r'C:\NHLdb_pyqt\files\_game_log_search_time.txt', "a")
                time_for_search.write("" + now_format + " - GameLogData - Search time for " + search_text[0] + " took " + str(search_time) + " seconds\n")
                time_for_search.close()
        # quit driver
        driver.quit()
        # return league_bit for edit function
        return league_bit


class EditGameLogExport:
    def colhockeyinc_game_logs(self, search_text):
        # open file
        df1 = pd.read_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + ' ' + search_text[1] + '.xlsx')
        # rename columns
        df1.columns = ['', 'Date', 'Opponent', 'Result', 'Points', 'GW', 'PP Points', 'SH Points', 'G Streak',
                       'A Streak', 'Point Streak', 'P/Min', 'SOG', '+/-', 'Season GP', 'Reverse GP']
        # check for "--- DID NOT DRESS ---" in 'Points' column, if it's there replace string
        if df1['Points'].str.contains('---- DID NOT DRESS ----').any():
            df1['Points'] = df1['Points'].replace({'---- DID NOT DRESS ----': 'DNP-DNP-DNP'})
        # check for "E" in '+/-" column, if it's there replace string
        if df1['+/-'].str.contains('E').any():
            df1['+/-'] = df1['+/-'].replace({'E': '0'})
        # split PP points, SH points column, ES points, +/- column
        df1[['PP G', 'PP A', 'PP Total']] = df1['PP Points'].str.split('-', expand=True)
        df1[['SH G', 'SH A', 'SH Total']] = df1['SH Points'].str.split('-', expand=True)
        df1[['G', 'A', 'Total']] = df1['Points'].str.split('-', expand=True)
        df1[['P', 'Min']] = df1['P/Min'].str.split('/', expand=True)
        # drop a bunch of columns
        df1.drop('PP Points', axis=1, inplace=True)
        df1.drop('SH Points', axis=1, inplace=True)
        df1.drop('Points', axis=1, inplace=True)
        df1.drop('P/Min', axis=1, inplace=True)
        df1.drop('Point Streak', axis=1, inplace=True)
        df1.drop('Season GP', axis=1, inplace=True)
        df1.drop('Reverse GP', axis=1, inplace=True)
        # drop first row, last row
        df1.drop(0, inplace=True)
        df1.drop(df1.tail(1).index, inplace=True)
        # re-order columns
        df1 = df1[['Date', 'Opponent', 'Result', 'G', 'A', 'Total', 'P', 'Min', 'SOG', '+/-', 'GW', 'PP G', 'PP A',
                   'PP Total', 'SH G', 'SH A', 'SH Total']]
        # convert data types
        cols = ['G', 'A', 'Total', 'P', 'Min', 'SOG', '+/-', 'PP G', 'PP A', 'PP Total', 'SH G', 'SH A', 'SH Total']
        df1[cols] = df1[cols].apply(pd.to_numeric, errors='coerce', axis=1)
        # refill DNP and Exhibitions
        if df1['G'].isnull().values.any():
            df1[['G', 'A', 'Total']] = df1[['G', 'A', 'Total']].fillna('DNP')
        if df1['SOG'].isnull().values.any():
            df1[['SOG']] = df1[['SOG']].fillna('Exhibition')
        # fill N/As with zeros
        df1.fillna(0, inplace=True)
        # save data file
        df1.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + ' ' + search_text[1] + '.xlsx')


class InsertIntoDatabase:
    def colhockeyinc_insert_log(self, search_text):
        # connect to database, return connection/cursor
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # Open game log data
        df1 = pd.read_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + ' ' + search_text[1] + '.xlsx')
        # insert table if it doesn't exist
        table_name = search_text[0].replace(" ", "")
        cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + " (Date DATE NOT NULL, "
                                                                    "Season TEXT NOT NULL, "
                                                                    "Team TEXT NOT NULL, "
                                                                    "League TEXT NOT NULL, "
                                                                    "Opponent TEXT NOT NULL, "
                                                                    "Result TEXT NOT NULL, "
                                                                    "Goals TEXT NOT NULL, "
                                                                    "Assists TEXT NOT NULL, "
                                                                    "Total TEXT NOT NULL, "
                                                                    "Penalties TINYINT NOT NULL, "
                                                                    "PIM TINYINT NOT NULL, "
                                                                    "SOG TEXT NOT NULL, "
                                                                    "PlusMinus TINYINT NOT NULL, "
                                                                    "GW TEXT NOT NULL, "
                                                                    "PPG TINYINT NOT NULL, "
                                                                    "PPA TINYINT NOT NULL, "
                                                                    "PPTotal TINYINT NOT NULL, "
                                                                    "SHG TINYINT NOT NULL, "
                                                                    "SHA TINYINT NOT NULL, "
                                                                    "SHTotal TINYINT NOT NULL, "
                                                                    "PRIMARY KEY (Date))")
        # 1. check to make sure data (primary key) doesn't exists to avoid redundant data
        # 2. insert data row by row
        list_of_rows = df1.to_numpy().tolist()
        for row in range(len(list_of_rows)):
            # get date
            date = list_of_rows[row][1]
            check_date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            #   check for repeat date
            query = "SELECT Date FROM " + table_name + " WHERE Date = %s"
            cursor.execute(query, (check_date,))
            numb = cursor.fetchall()
            count = len(numb)
            if count > 0:
                # date already exists, skip
                continue
            else:
                # unpack rest of data
                season = search_text[1]
                team = search_text[2]
                league = search_text[3]
                opp = list_of_rows[row][2]
                result = list_of_rows[row][3]
                goal = list_of_rows[row][4]
                assist = list_of_rows[row][5]
                ptotal = list_of_rows[row][6]
                pen = list_of_rows[row][7]
                pim = list_of_rows[row][8]
                plusminus = list_of_rows[row][9]
                sog = list_of_rows[row][10]
                gw = list_of_rows[row][11]
                ppg = list_of_rows[row][12]
                ppa = list_of_rows[row][13]
                pptot = list_of_rows[row][14]
                shg = list_of_rows[row][15]
                sha = list_of_rows[row][16]
                sht = list_of_rows[row][17]
                # insert row into database
                sql = "INSERT INTO " + table_name + " (Date, Season, Team, League, Opponent, Result, Goals, Assists, Total, Penalties, PIM, " \
                                                    "SOG, PlusMinus, GW, PPG, PPA, PPTotal, SHG, SHA, SHTotal)" \
                    "VALUES (STR_TO_DATE(%s, '%m/%d/%Y'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (date, season, team, league, opp, result, goal, assist, ptotal, pen, pim, plusminus, sog, gw, ppg, ppa, pptot, shg, sha, sht)
                cursor.execute(sql, val)
                connection.commit()
        # close database connection
        close_db_connection(connection)
