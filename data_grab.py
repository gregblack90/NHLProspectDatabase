from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
from functools import partial
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import mysql.connector as mysql
import dbConfig as guiConfig
from game_log_functions import GameLogSearch, EditGameLogExport, InsertIntoDatabase


# Open Database connection
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return[conn, curs]


# Close Database connection
def close_db_connection(conn):
    conn.close()


# Setting up GUI
class UiSetup:
    def populate_lists(self):
        # Team List
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        cursor.execute("SELECT * FROM teams")
        result = cursor.fetchall()
        teams = []
        for x in result:
            teams.append(x[0])
        # close connection
        close_db_connection(connection)

        # Initial Player List
        player_list_init = "Please Select a Team"
        self.team_select_combobox.clear()
        self.team_select_combobox.addItems(teams)
        self.player_select_combobox.addItem(player_list_init)

    # Change Prospect List based on Team Selection
    def change_prospect_list(self):
        index = self.team_select_combobox.currentText()
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        query = """SELECT Name, Position FROM prospects WHERE Team = %s ORDER BY Position, Name"""
        cursor.execute(query, (index,))
        result = cursor.fetchall()
        players = []
        # players found, populate list
        if len(result) != 0:
            for x in result:
                players.append(x[1] + ": " + x[0])
            self.player_select_combobox.clear()
            self.player_select_combobox.addItems(players)
        # if nothing found in database
        if len(result) == 0:
            self.player_select_combobox.clear()
            self.player_select_combobox.addItem("Prospects Not Compiled")
        close_db_connection(connection)

    # Clear Table contents
    def clear_table(self):
        self.tableWidget.setRowCount(0)


# Calling web scrape to get season/gp data
class SeasonScrape:

    # Call thread for web scrape when button is clicked (long running task)
    def call_scrape_thread(self):
        # Create thread object
        self.thread = QThread(parent=self)
        # Create worker object
        self.worker = SeasonData()
        # Move worker to thread
        self.worker.moveToThread(self.thread)
        # Get prospect name before calling thread
        prospect = self.player_select_combobox.currentText()
        # Connect signals and slots
        self.thread.started.connect(partial(self.worker.get_season_data, prospect))
        self.worker.finished.connect(self.thread.quit)
        # self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        # Clear table
        self.tableWidget.setRowCount(0)
        # Start the thread
        self.progressBar.setValue(99.9)
        self.progressBar.setFormat("Loading Player Data...Please Wait.")
        self.thread.start()
        self.worker.percent_changed.connect(self.progressBar.setValue)
        self.worker.progress_bar_str.connect(self.progressBar.setFormat)
        # Final resets
        self.get_player_data_button.setEnabled(False)
        self.thread.finished.connect(lambda: self.get_player_data_button.setEnabled(True))
        self.thread.finished.connect(lambda: self.get_season_data_button.setEnabled(True))
        # Add Data to GUI
        self.worker.season_data.connect(self.add_to_table)


# Thread for season data web scrape
class SeasonData(QObject):
    # Thread Signals
    finished = pyqtSignal()
    season_data = pyqtSignal(list)
    percent_changed = pyqtSignal(int)
    progress_bar_str = pyqtSignal(str)

    # Get season data when button is clicked
    def get_season_data(self, prospect):
        # function used for data frame
        def row_get_data_text(table_row, col_tag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in table_row.find_all(col_tag)]
        # Start
        start = time.time()
        prospect = prospect[3:]
        webpage = "http://google.com/search?q=" + prospect
        options = Options()
        options.page_load_strategy = 'eager'
        options.add_argument('--headless')
        options.add_argument('--disable gpu')
        driver = webdriver.Chrome(executable_path=r"C:\chromedriver.exe", options=options)
        driver.get(webpage)
        # go to elite prospects page for player
        link = driver.find_element_by_xpath("//*[contains(text(), ' - Elite Prospects')]")
        link.click()
        # get team, league and GP for each year, return list
        table = "table table-striped table-condensed table-sortable player-stats skater-stats highlight-stats"
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
            data_table_df = pd.DataFrame(data[1:], columns=data[0])
            data_table_df.to_csv(r'C:\NHLdb_pyqt\data_frame_tests\season_select\season_select_' + prospect + '.csv')
            # find run time to get data
            end = time.time()
            search_time = end - start
            now = datetime.now()
            now_format = now.strftime("%m/%d/%Y %H:%M:%S")
            time_for_search = open(r'C:\NHLdb_pyqt\files\_season_search_time.txt', "a")
            time_for_search.write("" + now_format + " - SeasonData - Search time for " + prospect +
                                  " took " + str(search_time) + " seconds\n")
            time_for_search.close()
            # quit driver
            driver.quit()
            # trim data frame for only needed info
            data_table = pd.read_csv(r'C:\NHLdb_pyqt\data_frame_tests\season_select\season_select_' + prospect + '.csv')
            data_table = data_table.drop(data_table.columns[[0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]],
                                         axis=1)
            # fill column 0 to have years for all season
            data_table = data_table.fillna(method='ffill')
            # check if any "-" are in GP column, if so, replace with 0
            if data_table['GP'].dtype != 'int64':
                data_table['GP'] = data_table['GP'].str.replace('-', '0', regex=True)
            # save data frame
            data_table.to_csv(r'C:\NHLdb_pyqt\data_frame_tests\season_select\season_select_' + prospect + '.csv')
            # get lists from dataframe
            season_list = data_table['S'].values.tolist()
            team_list = data_table['Team'].values.tolist()
            league_list = data_table['League'].values.tolist()
            gp_list = data_table['GP'].values.tolist()
            append_data = [season_list, team_list, league_list, gp_list]
            self.season_data.emit(append_data)
            self.finished.emit()
            # reset progress bar formatting
            self.percent_changed.emit(0)
            self.progress_bar_str.emit("")


# Calling web scrape to get game log data
class PlayerSeasonData:
    def get_player_data(self):
        year = []
        team = []
        league = []
        for index in self.tableWidget.selectionModel().selectedRows():
            row = index.row()
            year.append(self.tableWidget.item(row, 0).text())
            # need to check team for A or C in string
            team_chk = self.tableWidget.item(row, 1).text()
            if team_chk.endswith('”'):
                team_chk = team_chk[:-3]
                team.append(team_chk)
            else:
                team.append(team_chk)
            league.append(self.tableWidget.item(row, 2).text())

        # Create thread object
        self.thread = QThread(parent=self)
        # Create worker object
        self.worker = PlayerDataScrape()
        # Move worker to thread
        self.worker.moveToThread(self.thread)
        # Connect signals and slots
        prospect = self.player_select_combobox.currentText()
        self.thread.started.connect(partial(self.worker.player_data_scrape, prospect, year, team, league))
        self.worker.finished.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)
        # Start the thread
        self.progressBar.setValue(99.9)
        self.progressBar.setFormat("Loading Game Log Data...Please Wait.")
        self.thread.start()
        self.worker.percent_changed.connect(self.progressBar.setValue)
        self.worker.progress_bar_str.connect(self.progressBar.setFormat)
        self.get_season_data_button.setEnabled(False)
        # Final resets
        self.thread.finished.connect(lambda: self.get_season_data_button.setEnabled(True))


# Thread for game log data web scrape
class PlayerDataScrape(QObject):
    # Thread Signals
    finished = pyqtSignal()
    percent_changed = pyqtSignal(int)
    progress_bar_str = pyqtSignal(str)

    def player_data_scrape(self, prospect, year, team, league):
        search_text = []
        for i in range(len(year)):
            # assemble search text list
            search_text.append(prospect[3:])
            search_text.append(year[i])
            # add condition for USNTDP in USHL
            if team[i] == "USNTDP Juniors":
                search_text.append("Team USA")
            else:
                search_text.append(team[i])
            search_text.append(league[i])

            # call search function
            call_game_log = GameLogSearch()
            league_bit = call_game_log.game_log_search(search_text)

            # call game log edit function
            edit_game_log = EditGameLogExport()
            #   function will depend on league/game_log bit
            if league_bit == 1:  # NCAA (CollegeHockeyInc)
                edit_game_log.colhockeyinc_game_logs(search_text)
            if league_bit == 2:  # USHL
                edit_game_log.ushl_game_log(search_text)

            # call insert data into database
            insert_game_log = InsertIntoDatabase()
            insert_game_log.insert_log(search_text, league_bit)

            # clear list, reset league bit and go to next selection
            search_text = []
        # Finished
        self.finished.emit()
        # reset progress bar formatting
        self.percent_changed.emit(0)
        self.progress_bar_str.emit("")
