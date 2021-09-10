from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from datetime import datetime
import mysql.connector as mysql
import dbConfig as guiConfig


# Open Database connection
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return [conn, curs]


# Close Database connection
def close_db_connection(conn):
    conn.close()


class GameLogSearch:
    @staticmethod
    # set_league_bit_get_webpage and get_game_log_table functions are called from this function
    def game_log_search(search_text):
        # Start
        start = time.time()

        # call function for league bit and webpage
        search_info = GameLogSearch.set_league_bit_get_webpage(search_text)
        league_bit = search_info[0]
        webpage = search_info[1]
        if league_bit != 0:
            # common web scrape items
            options = Options()
            options.page_load_strategy = 'eager'
            # options.add_argument('--headless')
            # options.add_argument('--disable gpu')
            driver = webdriver.Chrome(executable_path=r"C:\chromedriver.exe", options=options)
            driver.get(webpage)

            # call function to get game log table
            GameLogSearch.get_game_log_table(league_bit, search_text, driver)

            # find run time to get data
            end = time.time()
            search_time = end - start
            now = datetime.now()
            now_format = now.strftime("%m/%d/%Y %H:%M:%S")
            # write to log file
            time_for_search = open(r'C:\NHLdb_pyqt\files\_game_log_search_time.txt', "a")
            time_for_search.write(
                "" + now_format + " - GameLogData - " + search_text[3] + " - Search time for " + search_text[0] +
                " took " + str(search_time) + " seconds\n")
            time_for_search.close()
            # quit driver
            driver.quit()
        # return league_bit for edit function
        return league_bit

    @staticmethod
    def set_league_bit_get_webpage(search_text):
        # different teams will have different google searches
        webpage = ""
        if search_text[3] == "NCAA" or search_text[3] == "AHA" or search_text[3] == "Big-10" or search_text[3] == "ECAC"\
                or search_text[3] == "NCHC" or search_text[3] == "WCHA" or search_text[3] == "H-East":
            # set league bit
            league_bit = 1
            year = search_text[1][-2:]
            if search_text[2] == "Air Force Academy":
                webpage = "http://collegehockeyinc.com/teams/air-force/roster" + year + ".php"
            elif search_text[2] == "American International College":
                webpage = "http://collegehockeyinc.com/teams/american-international/roster" + year + ".php"
            elif search_text[2] == "Arizona State Univ." or search_text[2] == "Arizona State University":
                year = search_text[1][-2:]
                webpage = "http://collegehockeyinc.com/teams/arizona-state/roster" + year + ".php"
            elif search_text[2] == "Army (U.S. Military Academy)" or search_text[2] == "Army":
                webpage = "http://collegehockeyinc.com/teams/army-west-point/roster" + year + ".php"
            elif search_text[2] == "Bemidji State Univ." or search_text[2] == "Bemidji State University":
                webpage = "http://collegehockeyinc.com/teams/bemidji-state/roster" + year + ".php"
            elif search_text[2] == "Bentley Univ." or search_text[2] == "Bentley University":
                webpage = "http://collegehockeyinc.com/teams/bentley/roster" + year + ".php"
            elif search_text[2] == "Boston College":
                webpage = "https://collegehockeyinc.com/teams/boston-college/roster" + year + ".php"
            elif search_text[2] == "Boston Univ." or search_text[2] == "Boston University":
                webpage = "http://collegehockeyinc.com/teams/boston-university/roster" + year + ".php"
            elif search_text[2] == "Bowling Green State Univ." or search_text[2] == "Bowling Green State University":
                webpage = "http://collegehockeyinc.com/teams/bowling-green/roster" + year + ".php"
            elif search_text[2] == "Brown Univ." or search_text[2] == "Brown University":
                webpage = "http://collegehockeyinc.com/teams/brown/roster" + year + ".php"
            elif search_text[2] == "Canisius College":
                webpage = "http://collegehockeyinc.com/teams/canisius/roster" + year + ".php"
            elif search_text[2] == "Clarkson Univ." or search_text[2] == "Clarkson University":
                webpage = "http://collegehockeyinc.com/teams/clarkson/roster" + year + ".php"
            elif search_text[2] == "Colgate Univ." or search_text[2] == "Colgate University":
                webpage = "http://collegehockeyinc.com/teams/colgate/roster" + year + ".php"
            elif search_text[2] == "College of the Holy Cross" or search_text[2] == "Holy Cross College":
                webpage = "http://collegehockeyinc.com/teams/holy-cross/roster" + year + ".php"
            elif search_text[2] == "Colorado College":
                webpage = "http://collegehockeyinc.com/teams/colorado-college/roster" + year + ".php"
            elif search_text[2] == "Cornell Univ." or search_text[2] == "Cornell University":
                webpage = "http://collegehockeyinc.com/teams/cornell/roster" + year + ".php"
            elif search_text[2] == "Dartmouth College":
                webpage = "http://collegehockeyinc.com/teams/dartmouth/roster" + year + ".php"
            elif search_text[2] == "Ferris State Univ." or search_text[2] == "Ferris State University":
                webpage = "http://collegehockeyinc.com/teams/ferris-state/roster" + year + ".php"
            elif search_text[2] == "Harvard Univ." or search_text[2] == "Harvard University":
                webpage = "http://collegehockeyinc.com/teams/harvard/roster" + year + ".php"
            elif search_text[2] == "Lake Superior State Univ." or search_text[2] == "Lake Superior State University":
                webpage = "http://collegehockeyinc.com/teams/lake-superior-state/roster" + year + ".php"
            elif search_text[2] == "Long Island Univ.":
                webpage = "http://collegehockeyinc.com/teams/long-island-university/roster" + year + ".php"
            elif search_text[2] == "Mercyhurst Univ." or search_text[2] == "Mercyhurst College":
                webpage = "http://collegehockeyinc.com/teams/mercyhurst/roster" + year + ".php"
            elif search_text[2] == "Merrimack College":
                webpage = "http://collegehockeyinc.com/teams/merrimack/roster" + year + ".php"
            elif search_text[2] == "Miami Univ. (Ohio)" or search_text[2] == "Miami University (Ohio)":
                webpage = "http://collegehockeyinc.com/teams/miami/roster" + year + ".php"
            elif search_text[2] == "Michigan State Univ." or search_text[2] == "Michigan State University":
                webpage = "http://collegehockeyinc.com/teams/michigan-state/roster" + year + ".php"
            elif search_text[2] == "Michigan Tech":
                webpage = "http://collegehockeyinc.com/teams/michigan-tech/roster" + year + ".php"
            elif search_text[2] == "Minnesota State Univ. (Mankato)" or search_text[2] == "Minnesota State U - Mankato":
                webpage = "http://collegehockeyinc.com/teams/minnesota-state/roster" + year + ".php"
            elif search_text[2] == "Niagara Univ." or search_text[2] == "Niagara University":
                webpage = "http://collegehockeyinc.com/teams/niagara/roster" + year + ".php"
            elif search_text[2] == "Northeastern Univ." or search_text[2] == "Northeastern University":
                webpage = "http://collegehockeyinc.com/teams/northeastern/roster" + year + ".php"
            elif search_text[2] == "Northern Michigan Univ." or search_text[2] == "Northern Michigan University":
                webpage = "http://collegehockeyinc.com/teams/northern-michigan/roster" + year + ".php"
            elif search_text[2] == "Ohio State Univ." or search_text[2] == "Ohio State University":
                webpage = "http://collegehockeyinc.com/teams/ohio-state/roster" + year + ".php"
            elif search_text[2] == "Penn State Univ." or search_text[2] == "Pennsylvania State U":
                webpage = "http://collegehockeyinc.com/teams/penn-state/roster" + year + ".php"
            elif search_text[2] == "Princeton Univ." or search_text[2] == "Princeton University":
                webpage = "http://collegehockeyinc.com/teams/princeton/roster" + year + ".php"
            elif search_text[2] == "Providence College":
                webpage = "http://collegehockeyinc.com/teams/providence/roster" + year + ".php"
            elif search_text[2] == "Quinnipiac Univ." or search_text[2] == "Quinnipiac University":
                webpage = "http://collegehockeyinc.com/teams/quinnipiac/roster" + year + ".php"
            elif search_text[2] == "RIT (Rochester Inst. of Tech.)" or search_text[2] == "R.I.T.":
                webpage = "http://collegehockeyinc.com/teams/rit/roster" + year + ".php"
            elif search_text[2] == "Robert Morris Univ." or search_text[2] == "Robert Morris University":
                webpage = "http://collegehockeyinc.com/teams/robert-morris/roster" + year + ".php"
            elif search_text[2] == "RPI (Rensselaer Polytech. Inst.)" or search_text[2] == "R.P.I":
                webpage = "http://collegehockeyinc.com/teams/rensselaer/roster" + year + ".php"
            elif search_text[2] == "Sacred Heart Univ." or search_text[2] == "Sacred Heart University":
                webpage = "http://collegehockeyinc.com/teams/sacred-heart/roster" + year + ".php"
            elif search_text[2] == "St. Cloud State Univ." or search_text[2] == "St. Cloud State":
                webpage = "http://collegehockeyinc.com/teams/st-cloud-state/roster" + year + ".php"
            elif search_text[2] == "St. Lawrence Univ." or search_text[2] == "St. Lawrence University":
                webpage = "http://collegehockeyinc.com/teams/st-lawrence/roster" + year + ".php"
            elif search_text[2] == "UMass (Amherst)" or search_text[2] == "UMass-Amherst":
                webpage = "http://collegehockeyinc.com/teams/massachusetts/roster" + year + ".php"
            elif search_text[2] == "UMass-Lowell":
                webpage = "http://collegehockeyinc.com/teams/umass-lowell/roster" + year + ".php"
            elif search_text[2] == "Union College":
                webpage = "http://collegehockeyinc.com/teams/union/roster" + year + ".php"
            elif search_text[2] == "Univ. of Alabama-Huntsville" or search_text[2] == "U. of Alabama-Huntsville":
                webpage = "http://collegehockeyinc.com/teams/alabama-huntsville/roster" + year + ".php"
            elif search_text[2] == "Univ. of Alaska-Anchorage" or search_text[2] == "U. of Alaska-Anchorage":
                webpage = "http://collegehockeyinc.com/teams/alaska-anchorage/roster" + year + ".php"
            elif search_text[2] == "Univ. of Alaska-Fairbanks" or search_text[2] == "U. of Alaska-Fairbanks":
                webpage = "http://collegehockeyinc.com/teams/alaska/roster" + year + ".php"
            elif search_text[2] == "Univ. of Connecticut" or search_text[2] == "U. of Connecticut":
                webpage = "http://collegehockeyinc.com/teams/uconn/roster" + year + ".php"
            elif search_text[2] == "Univ. of Denver" or search_text[2] == "U. of Denver":
                webpage = "http://collegehockeyinc.com/teams/denver/roster" + year + ".php"
            elif search_text[2] == "Univ. of Maine" or search_text[2] == "U. of Maine":
                webpage = "http://collegehockeyinc.com/teams/maine/roster" + year + ".php"
            elif search_text[2] == "Univ. of Michigan" or search_text[2] == "U. of Michigan":
                webpage = "http://collegehockeyinc.com/teams/michigan/roster" + year + ".php"
            elif search_text[2] == "Univ. of Minnesota" or search_text[2] == "U. of Minnesota":
                webpage = "https://collegehockeyinc.com/teams/minnesota/roster" + year + ".php"
            elif search_text[2] == "Univ. of Minnesota-Duluth" or search_text[2] == "U. of Minnesota-Duluth":
                webpage = "http://collegehockeyinc.com/teams/minnesota-duluth/roster" + year + ".php"
            elif search_text[2] == "Univ. of Nebraska-Omaha" or search_text[2] == "U. of Nebraska-Omaha":
                webpage = "http://collegehockeyinc.com/teams/omaha/roster" + year + ".php"
            elif search_text[2] == "Univ. of New Hampshire" or search_text[2] == "U. of New Hampshire":
                webpage = "http://collegehockeyinc.com/teams/new-hampshire/roster" + year + ".php"
            elif search_text[2] == "Univ. of North Dakota" or search_text[2] == "U. of North Dakota":
                webpage = "http://collegehockeyinc.com/teams/north-dakota/roster" + year + ".php"
            elif search_text[2] == "Univ. of Notre Dame" or search_text[2] == "Notre Dame":
                webpage = "https://collegehockeyinc.com/teams/notre-dame/roster" + year + ".php"
            elif search_text[2] == "Univ. of Vermont" or search_text[2] == "U. of Vermont":
                webpage = "http://collegehockeyinc.com/teams/vermont/roster" + year + ".php"
            elif search_text[2] == "Univ. of Wisconsin" or search_text[2] == "U. of Wisconsin":
                webpage = "http://collegehockeyinc.com/teams/wisconsin/roster" + year + ".php"
            elif search_text[2] == "Western Michigan Univ." or search_text[2] == "Western Michigan University":
                webpage = "http://collegehockeyinc.com/teams/western-michigan/roster" + year + ".php"
            elif search_text[2] == "Yale Univ." or search_text[2] == "Yale University":
                webpage = "http://collegehockeyinc.com/teams/yale/roster" + year + ".php"
        elif search_text[3] == "USHL":
            # set league bit
            league_bit = 2
            webpage = "https://www.ushl.com/view#/roster/11/73"
        elif search_text[3] == "QMJHL":
            league_bit = 3
            webpage = "https://theqmjhl.ca/stats/players/196"
        elif search_text[3] == "OHL":
            league_bit = 4
            webpage = "https://ontariohockeyleague.com/stats/players/68"
        elif search_text[3] == "WHL":
            league_bit = 5
            webpage = "https://whl.ca/stats/players/270"
        # elif search_text[3] == "AHL":
        #     league_bit = 6
        #     webpage = "https://theahl.com/stats/player-stats"
        # search for team doesnt exist
        else:
            league_bit = 0
        return league_bit, webpage

    @staticmethod
    def get_game_log_table(league_bit, search_text, driver):
        # function used for data frame
        def row_get_data_text(table_row, col_tag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in table_row.find_all(col_tag)]

        # go to stats page for player and save data frame of game logs
        # NCAA
        if league_bit == 1:
            # navigate to stats table
            driver.find_element_by_partial_link_text(search_text[0]).click()
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
                data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_'
                                       + search_text[1] + '_' + search_text[2] + '_' + search_text[3] + '.xlsx')
        # USHL
        if league_bit == 2:
            # select year and team from user selection, click SUBMIT button
            time.sleep(2)
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                        "//select[@class='ng-pristine ng-untouched "
                                                                        "ng-valid ng-not-empty' and "
                                                                        "@ng-model='selectedSeason']//option[@label='" +
                                                                        search_text[1] + "']"))).click()
            time.sleep(2)
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                        "//select[@class='ng-pristine ng-untouched "
                                                                        "ng-valid ng-not-empty' and @ng-model="
                                                                        "'selectedTeamNoAll']//option[@label='" +
                                                                        search_text[2] + "']"))).click()
            driver.find_element_by_partial_link_text("SUBMIT").click()
            # select player
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.LINK_TEXT, "" + search_text[0] + ""))).click()
            time.sleep(2)
            # find stats table
            table = "ht-table"
            soup = BeautifulSoup(driver.page_source, "html.parser")
            stats_table = soup.find_all("table", {"class": table})
            counter = 0
            for table_num in stats_table:
                data = []
                if table_num is None:
                    # if table not found, try again
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    table_num = soup.find("table", {"class": table})
                if table_num is not None:
                    # table found, start getting data
                    trs = table_num.find_all('tr')
                    header_row = row_get_data_text(trs[0], 'th')
                    if header_row:  # if there is a header row include first
                        data.append(header_row)
                        trs = trs[1:]
                    for tr in trs:  # for every table row
                        data.append(row_get_data_text(tr, 'td'))  # data row
                    # make data frame, save to .csv (ONLY SAVE 2ND TABLE)
                    if counter == 0:
                        counter = counter + 1
                        continue
                    else:
                        data_table_df = pd.DataFrame(data)
                        data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0]
                                               + '_' + search_text[1] + '_' + search_text[2] + '_'
                                               + search_text[3] + '.xlsx')
        # QMJHL
        if league_bit == 3:
            # split team name and add comma to middle
            split_team = search_text[2].split(" ")
            select_team = split_team[0] + ", " + split_team[1]
            # need to split name and rearrange to "Last, First" because of webpage
            split_name = search_text[0].split(" ")
            click_name = split_name[1] + ", " + split_name[0]
            # select team
            dropdown_team = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.2.0.0']"))
            dropdown_team.select_by_visible_text(select_team)
            # select season
            dropdown_season = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.2.0.0']"))
            dropdown_season.select_by_visible_text(search_text[1] + " | Regular Season")
            # wait a second for it to load
            time.sleep(1)
            # click player name
            driver.find_element_by_partial_link_text(click_name).click()
            # once on player page, click "Game by Game" tab
            driver.find_element_by_class_name("sgamebygame").click()
            # have to select season again...
            select_again = Select(driver.find_element_by_id('season_id2'))
            time.sleep(2)
            select_again.select_by_visible_text(search_text[1] + " | Regular Season")
            # find stats table
            time.sleep(2)
            table = "controlBar"
            data = []
            soup = BeautifulSoup(driver.page_source, "html.parser")
            stats_table = soup.find("table", {"id": table})
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
                data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_'
                                       + search_text[1] + '_' + search_text[2] + '_' + search_text[3] + '.xlsx')
        # OHL
        if league_bit == 4:
            # need to split name and rearrange to "Last, First" because of webpage
            split_name = search_text[0].split(" ")
            click_name = split_name[1] + ", " + split_name[0]
            # select season
            dropdown_season = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.0.0.0']"))
            dropdown_season.select_by_visible_text(search_text[1] + " Regular Season")
            # select team
            dropdown_team = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.2.0.0']"))
            dropdown_team.select_by_visible_text(search_text[2])
            # click player name
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, click_name))).click()
            # once on player page, select game by game button
            driver.find_element_by_xpath("//a[@data-reactid='.0.0.0.2.0.$game_by_game-tab.$game_by_game-link']").click()
            # have to select season again...
            select_again = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.0.3.0.1.0.0.0']"))
            time.sleep(2)
            select_again.select_by_visible_text(search_text[1] + " Regular Season")
            # find stats table
            time.sleep(2)
            table = "stats-data-table table"
            data = []
            soup = BeautifulSoup(driver.page_source, "html.parser")
            stats_table = soup.find("table", {"id": table})
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
                data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_'
                                       + search_text[1] + '_' + search_text[2] + '_' + search_text[3] + '.xlsx')
        # WHL
        if league_bit == 5:
            # need to split name and rearrange to "Last, First" because of webpage
            split_name = search_text[0].split(" ")
            click_name = split_name[1] + ", " + split_name[0]
            # need to add space on either side of "-" in year...I know...ridiculous
            year = search_text[1]
            search_year = year[0:4] + " " + year[4] + " " + year[5:7]
            # select team
            dropdown_team = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.2.0.0']"))
            dropdown_team.select_by_visible_text(search_text[2])
            # select season
            time.sleep(2)
            dropdown_season = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.3.2.0.0']"))
            dropdown_season.select_by_visible_text(search_year + " Regular Season")
            # click player name
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, click_name))).click()
            # once on player page, select game by game button
            driver.find_element_by_xpath("//a[@data-reactid='.0.0.0.2.0.$game_by_game-tab.$game_by_game-link']").click()
            # have to select season again...
            select_again = Select(driver.find_element_by_xpath("//select[@data-reactid='.0.0.0.3.0.1.0.0.0']"))
            time.sleep(2)
            select_again.select_by_visible_text(search_year + " Regular Season")
            # find stats table
            time.sleep(2)
            table = "stats-data-table table"
            data = []
            soup = BeautifulSoup(driver.page_source, "html.parser")
            stats_table = soup.find("table", {"id": table})
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
                data_table_df.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_'
                                       + search_text[1] + '_' + search_text[2] + '_' + search_text[3] + '.xlsx')


class EditGameLogExport:
    @staticmethod
    def mult_teams_same_league_same_year(search_text, df1):
        df1.reset_index(drop=True, inplace=True)
        start = int(search_text[5])
        amt = int(search_text[4])
        keep_rows = range(start, (start + amt))
        df1 = df1[df1.index.isin(keep_rows)]
        return df1

    @staticmethod
    def colhockeyinc_game_logs(search_text):
        # open file
        df1 = pd.read_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')
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
        df1.to_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')

    @staticmethod
    def ushl_game_log(search_text):
        # open file
        df1 = pd.read_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')
        # rename columns
        df1.columns = ['', 'Opponent', 'Date', 'G', 'A', 'Total', 'SOG', 'Min', '+/-', 'PP G', 'SH G', 'SO G', 'GW',
                       'TG']
        # Drop first row (column headers), two unused columns
        df1.drop(0, inplace=True)
        df1.drop('SO G', axis=1, inplace=True)
        df1.drop('TG', axis=1, inplace=True)
        # replace any GWGs with G
        if df1['GW'].str.contains('1').any():
            df1['GW'] = df1['GW'].replace({'1': 'G'})
        # add columns to match table in database
        df1['Result'] = 'x'
        df1['P'] = 'x'
        df1['PP A'] = 'x'
        df1['PP Total'] = 'x'
        df1['SH A'] = 'x'
        df1['SH Total'] = 'x'
        df1 = df1[['Date', 'Opponent', 'Result', 'G', 'A', 'Total', 'P', 'Min', 'SOG', '+/-', 'GW', 'PP G', 'PP A',
                   'PP Total', 'SH G', 'SH A', 'SH Total']]
        # If len(search_text) == 6, player played for more than one team in the same league in the same league
        if len(search_text) == 6:
            df1 = EditGameLogExport.mult_teams_same_league_same_year(search_text, df1)
        # save data file
        df1.to_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')

    @staticmethod
    def qmjhl_game_log(search_text):
        # open file
        df1 = pd.read_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')
        # rename columns
        df1.columns = ['', 'Opponent', 'Date', 'G', 'A', 'Total', 'Min', '+/-', 'PP G', 'SH G', 'GW', 'TG', 'SOG',
                       'SH%', 'FO', 'WF%', 'SoG', 'SoGW', 'SSH', 'SSH%']
        # drop first two rows
        df1.drop([0, 1], inplace=True)
        # drop unneeded columns
        df1.drop('SoG', axis=1, inplace=True)
        df1.drop('SoGW', axis=1, inplace=True)
        df1.drop('SSH', axis=1, inplace=True)
        df1.drop('SSH%', axis=1, inplace=True)
        df1.drop('TG', axis=1, inplace=True)
        df1.drop('SH%', axis=1, inplace=True)
        df1.drop('FO', axis=1, inplace=True)
        df1.drop('WF%', axis=1, inplace=True)
        # get rid of "Totals" rows
        df1 = df1[~df1.Opponent.str.contains("Totals")]
        # replace "-" with '0'
        df1 = df1.replace({'-': '0'})
        # add needed columns for database consistency
        df1['Result'] = 'x'
        df1['P'] = 'x'
        df1['PP A'] = 'x'
        df1['PP Total'] = 'x'
        df1['SH A'] = 'x'
        df1['SH Total'] = 'x'
        # reorder columns
        df1 = df1[['Date', 'Opponent', 'Result', 'G', 'A', 'Total', 'P', 'Min', 'SOG', '+/-', 'GW', 'PP G', 'PP A',
                   'PP Total', 'SH G', 'SH A', 'SH Total']]
        # If len(search_text) == 6, player played for more than one team in the same league in the same league
        if len(search_text) == 6:
            df1 = EditGameLogExport.mult_teams_same_league_same_year(search_text, df1)
        # save edited file
        df1.to_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')

    @staticmethod
    def ohl_whl_game_log(search_text):
        # open file
        df1 = pd.read_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_'
                            + search_text[1] + '_' + search_text[2] + '_' + search_text[3] + '.xlsx')
        # rename column headers
        df1.columns = ['', 'Opponent', 'Date', 'G', 'A', 'Total', '+/-', 'Min', 'FOW', 'SOG']
        # drop first two rows
        df1.drop([0, 1], inplace=True)
        # drop FOW column
        df1.drop('FOW', axis=1, inplace=True)
        # reset row index numbers
        df1.reset_index(drop=True, inplace=True)
        # fill any NaN values with zeros
        df1.fillna(0, inplace=True)
        # convert PIM column to float then int to get rid of trailing zeros
        df1.Min = df1.Min.astype(float)
        df1.Min = df1.Min.astype(int)
        # add a bunch of columns
        df1['Result'] = 'x'
        df1['P'] = 'x'
        df1['PP A'] = 'x'
        df1['PP Total'] = 'x'
        df1['SH A'] = 'x'
        df1['SH Total'] = 'x'
        df1['GW'] = 'x'
        df1['PP G'] = 'x'
        df1['PP A'] = 'x'
        df1['SH G'] = 'x'
        # re-order columns
        df1 = df1[['Date', 'Opponent', 'Result', 'G', 'A', 'Total', 'P', 'Min', 'SOG', '+/-', 'GW', 'PP G', 'PP A',
                   'PP Total', 'SH G', 'SH A', 'SH Total']]
        # If len(search_text) == 6, player played for more than one team in the same league in the same league
        if len(search_text) == 6:
            df1 = EditGameLogExport.mult_teams_same_league_same_year(search_text, df1)
        # save edited file
        df1.to_excel(r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
                     + search_text[2] + '_' + search_text[3] + '.xlsx')


class InsertIntoDatabase:
    @staticmethod
    def insert_log(search_text, league_bit):
        # connect to database, return connection/cursor
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # Open game log data
        df1 = pd.read_excel(
            r'C:\NHLdb_pyqt\data_frame_tests\game_logs\game_log_' + search_text[0] + '_' + search_text[1] + '_'
            + search_text[2] + '_' + search_text[3] + '.xlsx')
        # insert table if it doesn't exist
        table_name = search_text[0].replace(" ", "")
        table_name_1 = table_name.replace("-", "")
        cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name_1 + " (Date DATE NOT NULL, "
                                                                      "Season TEXT NOT NULL, "
                                                                      "Team TEXT NOT NULL, "
                                                                      "League TEXT NOT NULL, "
                                                                      "Opponent TEXT NOT NULL, "
                                                                      "Result TEXT NOT NULL, "
                                                                      "Goals TEXT NOT NULL, "
                                                                      "Assists TEXT NOT NULL, "
                                                                      "Total TEXT NOT NULL, "
                                                                      "Penalties TEXT NOT NULL, "
                                                                      "PIM TEXT NOT NULL, "
                                                                      "SOG TEXT NOT NULL, "
                                                                      "PlusMinus TEXT NOT NULL, "
                                                                      "GW TEXT NOT NULL, "
                                                                      "PPG TEXT NOT NULL, "
                                                                      "PPA TEXT NOT NULL, "
                                                                      "PPTotal TEXT NOT NULL, "
                                                                      "SHG TEXT NOT NULL, "
                                                                      "SHA TEXT NOT NULL, "
                                                                      "SHTotal TEXT NOT NULL, "
                                                                      "PRIMARY KEY (Date))")
        # 1. check to make sure data (primary key) doesn't exists to avoid redundant data
        # 2. insert data row by row
        list_of_rows = df1.to_numpy().tolist()
        for row in range(len(list_of_rows)):
            # get date
            date = list_of_rows[row][1]
            check_date = ''
            if league_bit == 1:
                check_date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
            if league_bit == 2 or league_bit == 3 or league_bit == 4 or league_bit == 5:
                check_date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
            #   check for repeat date
            query = "SELECT Date FROM " + table_name_1 + " WHERE Date = %s"
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
                # sql statement needs to be different due to different DATE formatting for STR_TO_DATE function
                sql = ''
                if league_bit == 1:
                    sql = "INSERT INTO " + table_name_1 + "(Date, Season, Team, League, Opponent, Result, Goals, " \
                                                          "Assists, Total, Penalties, PIM, SOG, PlusMinus, GW, PPG, " \
                                                          "PPA, PPTotal, SHG, SHA, SHTotal)" \
                                                          "VALUES (STR_TO_DATE(%s, '%m/%d/%Y'), %s, %s, %s, %s, %s, " \
                                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                if league_bit == 2 or league_bit == 3 or league_bit == 4 or league_bit == 5:
                    sql = "INSERT INTO " + table_name_1 + "(Date, Season, Team, League, Opponent, Result, Goals, " \
                                                          "Assists, Total, Penalties, PIM, SOG, PlusMinus, GW, PPG, " \
                                                          "PPA, PPTotal, SHG, SHA, SHTotal)" \
                                                          "VALUES (STR_TO_DATE(%s, '%Y-%m-%d'), %s, %s, %s, %s, %s, " \
                                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (
                    date, season, team, league, opp, result, goal, assist, ptotal, pen, pim, plusminus, sog, gw, ppg,
                    ppa, pptot, shg, sha, sht)
                cursor.execute(sql, val)
                connection.commit()

        # 3. update UpdateTime table
        player = table_name_1
        update_date = datetime.today().strftime('%Y-%m-%d')
        t = time.localtime()
        update_time = str(time.strftime("%H:%M:%S", t))
        # check if player is in table
        cursor.execute('SELECT Player FROM update_time')
        res = cursor.fetchall()
        names = []
        for item in res:
            names.append(item[0])
        # if name is in table, update columns
        if table_name_1 in names:
            sql_update = 'UPDATE update_time SET Date = %s, Time = %s WHERE Player = %s'
            val_update = (update_date, update_time, player)
            cursor.execute(sql_update, val_update)
        # if name is not in table, insert row
        else:
            sql_insert = 'INSERT INTO update_time (Player, Date, Time) VALUES (%s, %s, %s)'
            val_insert = (player, update_date, update_time)
            cursor.execute(sql_insert, val_insert)
        connection.commit()
        # close database connection
        close_db_connection(connection)
