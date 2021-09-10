from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
from PyQt5.QtGui import QFont
from functools import partial
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from datetime import datetime
import re
import mysql.connector as mysql
from collections import defaultdict
import dbConfig as guiConfig
from game_log_functions import GameLogSearch, EditGameLogExport, InsertIntoDatabase


# Open Database connection
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return [conn, curs]


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
        # player = []
        # get list of players in database - if player is in database, set font to bold
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_list = []
        search_data = []
        for (table,) in tables:
            if table == "prospects" or table == "teams" or table == "update_time":
                continue
            table_list.append(table)
        # for each player in table_list, go to database and find last row to get search information
        for i in range(len(table_list)):
            # for search purposes, need to add space between first and last name
            name = re.sub(r"(?<=\w)([A-Z])", r" \1", table_list[i])
            # if there is more than two spaces in name (ie Jean Luc Foudy), add hyphen in first space
            space_counter = len(re.findall(r"[ \n]+", name))
            if space_counter == 2:
                name = "-".join(name.split(" ", 1))
            # assemble search data
            search_data.append(name)
        # players found, populate list
        if len(result) != 0:
            model = self.player_select_combobox.model()
            self.player_select_combobox.clear()
            for x in result:
                player = x[1] + ": " + x[0]
                item = QtGui.QStandardItem(player)
                if x[0] in search_data:
                    font = QFont("MS Shell Dlg 2", 9, QFont.Bold)
                else:
                    font = QFont("MS Shell Dlg 2", 10)
                item.setFont(font)
                model.appendRow(item)
                player = []
            # self.player_select_combobox.addItems(player)
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
        # Get website for search before calling thread
        if self.elite_radioButton.isChecked():
            website = "eliteprospects"
        elif self.hockeydb_radioButton.isChecked():
            website = "hockeydb"
        else:
            website = "hockeydb"
        # Connect signals and slots
        self.thread.started.connect(partial(self.worker.get_season_data, prospect, website))
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
    def get_season_data(self, prospect, website):
        # function used for data frame
        def row_get_data_text(table_row, col_tag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in table_row.find_all(col_tag)]

        # Start
        start = time.time()
        prospect = prospect[3:]
        webpage = "http://google.com/search?q=" + prospect
        options = Options()
        options.page_load_strategy = 'eager'
        # options.add_argument('--headless')
        # options.add_argument('--disable gpu')
        driver = webdriver.Chrome(executable_path=r"C:\chromedriver.exe", options=options)
        driver.get(webpage)
        # go to season data page for player
        if website == "eliteprospects":
            link = driver.find_element_by_xpath("//*[contains(text(), ' - Elite Prospects')]")
            link.click()
        elif website == "hockeydb":
            link = driver.find_element_by_xpath("//*[contains(text(), 'hockeydb.com')]")
            link.click()
        else:
            link = driver.find_element_by_xpath("//*[contains(text(), 'hockeydb.com')]")
            link.click()
        # wait one second and hit "ESC" which will stop loading page
        time.sleep(1)
        driver.find_element_by_xpath("//body").send_keys(Keys.ESCAPE)
        # get team, league and GP for each year, return list
        table = "sortable autostripe st reg"
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
            data_table_df = pd.DataFrame(data[1:])
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
            data_table.columns = ['', 'Season', 'Team', 'League', 'GP', '', '', '', '', '', '', '', '', '', '']
            data_table.drop(0, inplace=True)
            data_table.drop(data_table.columns[[0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]], axis=1, inplace=True)
            if data_table.GP.dtype == np.float64:
                data_table.GP = data_table.GP.apply(int)
            # save data frame
            data_table.to_csv(r'C:\NHLdb_pyqt\data_frame_tests\season_select\season_select_' + prospect + '.csv')
            # get lists from dataframe
            season_list = data_table['Season'].values.tolist()
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
        gp = []
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
            gp.append(self.tableWidget.item(row, 3).text())

        # Create thread object
        self.thread = QThread(parent=self)
        # Create worker object
        self.worker = PlayerDataScrape()
        # Move worker to thread
        self.worker.moveToThread(self.thread)
        # Connect signals and slots
        prospect = self.player_select_combobox.currentText()
        self.thread.started.connect(partial(self.worker.player_data_scrape, prospect, year, team, league, gp))
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

    def player_data_scrape(self, prospect, year, team, league, gp):
    # CREATE LIST OF PLAYER DETAILS FOR GAME LOG SEARCH LOOP
        search_text_total = []
        # check for repeat years
        repeat_year = []
        repeat_year_loc = []
        repeat_league = []
        duplicate_years = defaultdict(list)
        dup_leagues = defaultdict(list)
        for j, e in enumerate(year):
            duplicate_years[e].append(j)
        for k, f in enumerate(league):
            dup_leagues[f].append(k)
        for check, location in sorted(duplicate_years.items()):
            if len(location) >= 2:
                repeat_year.append(check)
                repeat_year_loc.append(location)
        for check, location in sorted(dup_leagues.items()):
            if len(location) >= 2:
                repeat_league.append(check)
        # assemble search text list, append to a new list that contains all data for player search
        for i in range(len(year)):
            search_text = []
            # assemble search text list
            search_text.append(prospect[3:])
            search_text.append(year[i])
            # add condition for USNTDP in USHL
            if team[i] == "USNTDP Juniors" or team[i][:19] == "U.S. National Under":
                search_text.append("Team USA")
            else:
                search_text.append(team[i])
            # add condition for NCAA leagues (for scraping from hockeydb)
            # if league[i] == "Big-10":
            #     search_text.append("NCAA")
            # else:
            search_text.append(league[i])
            # add GP number if there is a repeat season, else add zero
            if search_text[1] in repeat_year and search_text[3] in repeat_league:
                search_text.append(gp[i])
            else:
                search_text.append(0)
            search_text_total.append(search_text)


    # ACCOMMODATING FOR DIFFERENT TEAM/SAME LEAGUE/SAME YEAR
        # get length of repeated years list
        search_text_total.append("buffer")
        num_rep_years = len(repeat_year)
        num_of_years = 0
        # num_of_years will be incremented, go through the list of repeated years
        while num_of_years < num_rep_years:
            cur_year = repeat_year[num_of_years]  # get current year to search
            counter = 0  # set occurrence counter
            for count in range(len(search_text_total)):  # for the range of all player search information...
                # if there is a repeat year found,
                # and the year matches with the next items year
                # and the leagues match with the next item in list...
                if cur_year == search_text_total[count][1] \
                        and search_text_total[count][1] == search_text_total[count + 1][1] \
                        and search_text_total[count][3] == search_text_total[count + 1][3]:
                    if counter == 0:  # first occurrence, append beginning range of 0 to end of list
                        search_text_total[count].append("0")
                    else:  # any other occurrence, go to previous search text and append the number of GP + 1 to current list
                        beg_of_gm_rg = int(search_text_total[count - 1][4])
                        search_text_total[count].append(str(beg_of_gm_rg))
                    counter = counter + 1
                # if there is a repeat year found
                # and the years DON'T match with next item in list,
                # but matches with the PREVIOUS item in list,
                # and the league matches the previous item in the list,
                # it is the end of the matching year, get the amount of games played based on the total from previous matches (count)
                if cur_year == search_text_total[count][1] \
                        and search_text_total[count][1] != search_text_total[count + 1][1] \
                        and search_text_total[count][1] == search_text_total[count - 1][1] \
                        and search_text_total[count][3] == search_text_total[count - 1][3]:
                    gm_rg = 0
                    for item in range(counter):
                        gm_rg = gm_rg + int(search_text_total[count - (item + 1)][4])
                    search_text_total[count].append(str(gm_rg))
            num_of_years = num_of_years + 1
        # delete buffer
        del search_text_total[-1]


    # SEARCH FOR GAME LOG/EDIT GAME LOG/INSERT GAME LOG
        # getting data loop
        for search_data in search_text_total:
            print(search_data)
            # call search function (get game log data)
            call_game_log = GameLogSearch()
            league_bit = call_game_log.game_log_search(search_data)

            # call game log edit function
            edit_game_log = EditGameLogExport()
            #   function will depend on league/game_log bit
            if league_bit == 1:  # NCAA (CollegeHockeyInc)
                edit_game_log.colhockeyinc_game_logs(search_data)
            if league_bit == 2:  # USHL
                edit_game_log.ushl_game_log(search_data)
            if league_bit == 3:  # QMJHL
                edit_game_log.qmjhl_game_log(search_data)
            if league_bit == 4 or league_bit == 5:  # OHL/WHL - game logs are the same
                edit_game_log.ohl_whl_game_log(search_data)

            # call insert data into database function
            if league_bit != 0:
                insert_game_log = InsertIntoDatabase()
                insert_game_log.insert_log(search_data, league_bit)
            # if league bit is zero, alert league not found, alert user
            if league_bit == 0:
                print("League not programmed yet :(")

        # Finished
        self.finished.emit()
        # reset progress bar formatting
        self.percent_changed.emit(0)
        self.progress_bar_str.emit("")
