from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
import mysql.connector as mysql
import dbConfig as guiConfig
from datetime import date


# Open Database connection
#   --> return connection and cursor
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return [conn, curs]


# Close Database connection
def close_db_connection(conn):
    conn.close()


class UiDBsetup:
    def pop_table_list(self):
        # Call DB connection function, set returned values as connection/cursor
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # Select NHL database
        cursor.execute("USE NHL")
        # Get all tables
        cursor.execute("SHOW TABLES")
        result = cursor.fetchall()
        # Assemble table list
        table_names = []
        for x in result:
            res = str(x[0])
            table_names.append(res)
        # close DB connection
        close_db_connection(connection)
        # add table names to combobox
        self.select_table_list.addItems(table_names)


class DBFunctions:
    # function for error message if user tries to edit a table that is not prospects or teams
    @staticmethod
    def show_error(error_bit):
        def close_error_msg():
            error_msg.close()
            return
        error_msg = QtWidgets.QMessageBox()
        error_msg.setIcon(QtWidgets.QMessageBox.Warning)
        if error_bit == 1:
            error_msg.setText("Please select a row!")
        elif error_bit == 2:
            error_msg.setText("Cannot edit this table!")
        elif error_bit == 3:
            error_msg.setText("Cannot delete entries from this table!")
        error_msg.setWindowTitle("ERROR")
        error_msg.setStandardButtons(QtWidgets.QMessageBox.Ok)
        error_msg.buttonClicked.connect(close_error_msg)
        return error_msg.exec_()

    def load_table(self):
        # repopulate table list
        UiDBsetup.pop_table_list(self)
        # Get select table name
        table_name = self.select_table_list.currentText()
        # Call DB connection function, set returned values as connection/cursor
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # create empty data list
        data = []
        # clear any previous data
        self.view_database_table.setRowCount(0)
        # TEAMS TABLE
        if table_name == "teams":
            # Select all data from teams table
            cursor.execute("SELECT * FROM teams")
            result = cursor.fetchall()
            # append results to list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Team", "Conference", "Division",
                             "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
        # PROSPECTS TABLE
        elif table_name == "prospects":
            # Select all data from teams table
            cursor.execute("SELECT * FROM prospects")
            result = cursor.fetchall()
            # append results to list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Name", "Team", "Position", "Height", "Weight", "DOB", "Age", "Birthplace",
                             "", "", "", "", "", "", "", "", "", "", "", ""]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
        # UPDATE TIMES
        elif table_name == "update_time":
            # select all data from update_time table
            cursor.execute("SELECT * FROM update_time")
            result = cursor.fetchall()
            # append results to a list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Player", "Date", "Time", "", "", "", "", "",
                             "", "", "", "", "", "", "", "", "", "", "", ""]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
        # CURRENT TEAM
        elif table_name == "current_team":
            # select all data from update_time table
            cursor.execute("SELECT * FROM current_team")
            result = cursor.fetchall()
            # append results to a list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Player", "Team", "League", "", "", "", "", "",
                             "", "", "", "", "", "", "", "", "", "", "", ""]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
        # PROSPECT STATS
        # elif table_name != "teams" or table_name != "prospects":
        else:
            # Select all data from teams table
            cursor.execute("SELECT * FROM " + table_name + "")
            result = cursor.fetchall()
            # append results to list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Date", "Season", "Team", "League", "Opponent", "Result",
                             "Goals", "Assists", "Total Points",
                             "Penalties", "PIM", "SOG", "+/-", "GW",
                             "PPG", "PPA", "PP Total",
                             "SHG", "SHA", "SH Total"]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
        # enter data.  For each row...
        for row in range(len(data)):
            # get number of total rows...
            row_position = self.view_database_table.rowCount()
            # insert new row...
            self.view_database_table.insertRow(row_position)
            # for loop range will change with prospects table due to age being calculated and not in database
            if table_name == "prospects":
                range_len = len(data[row_position]) + 1
            else:
                range_len = len(data[row_position])
            # for each column in new row, insert data into columns
            for col in range(range_len):
                if col == 0:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(str(data[row_position][col])))
                if col == 1:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(str(data[row_position][col])))
                if col == 2:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 3:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 4:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 5:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 6:
                    # for the prospects table, calculate age
                    if table_name == "prospects":
                        dob = data[row_position][col-1]
                        b_mon = int(dob[0:2])
                        b_day = int(dob[3:5])
                        b_year = int(dob[6:10])
                        today = date.today()
                        age = today.year - b_year - ((today.month, today.day) < (b_mon, b_day))
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(str(age)))
                    else:
                        self.view_database_table.setItem(row_position, col,
                                                         QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 7:
                    # because age is calculated, index for birthplace needs to be subtracted from current column #
                    if table_name == "prospects":
                        self.view_database_table.setItem(row_position, col,
                                                         QtWidgets.QTableWidgetItem(data[row_position][col-1]))
                    else:
                        self.view_database_table.setItem(row_position, col,
                                                         QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 8:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 9:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 10:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 11:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 12:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 13:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 14:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 15:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 16:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 17:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 18:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 19:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
        # resize columns to fit data
        self.view_database_table.resizeColumnsToContents()
        # close database connection
        close_db_connection(connection)

    def update_entry(self):
        # Get select table name
        table_name = self.select_table_list.currentText()
        # if table is prospects or teams, go ahead and edit
        if table_name == "prospects" or table_name == "teams" or table_name == "current_team":
            # get select rows' contents
            data = []
            for index in self.view_database_table.selectionModel().selectedRows():
                row = index.row()
                # Can only edit teams/prospects/current team tables which have a max of 8 columns
                for column in range(8):
                    if self.view_database_table.item(row, column) is None:
                        # This will skip empty columns in team table
                        continue
                    else:
                        data.append(self.view_database_table.item(row, column).text())
            # if there is nothing in data, row was not selected, show error message
            if len(data) == 0:
                bit = 1
                table_error = DBFunctions.show_error(bit)
                if table_error > 0:  # quit function if you get here
                    return
            # connect to database
            db_conn = db_connection()
            connection = db_conn[0]
            cursor = db_conn[1]
            # edit data
            if table_name == "prospects":
                name = data[0]
                team = data[1]
                pos = data[2]
                height = data[3]
                weight = data[4]
                dob = data[5]
                age = data[6]
                bp = data[7]
                sql = "UPDATE " + table_name + " SET Team=%s ,Position=%s, Height=%s, Weight=%s, DOB=%s, " \
                                               "Birthplace=%s WHERE Name=%s"
                val = (team, pos, height, weight, dob, bp, name)
                cursor.execute(sql, val)
                connection.commit()
                close_db_connection(connection)
            elif table_name == "teams":
                team = data[0]
                conf = data[1]
                div = data[2]
                sql = "UPDATE " + table_name + " SET Conference=%s, Division=%s WHERE Team=%s"
                val = (conf, div, team)
                cursor.execute(sql, val)
                connection.commit()
                close_db_connection(connection)
            elif table_name == "current_team":
                player = data[0]
                team = data[1]
                league = data[2]
                sql = "UPDATE " + table_name + " SET Team=%s, League=%s WHERE Player=%s"
                val = (team, league, player)
                cursor.execute(sql, val)
                connection.commit()
                close_db_connection(connection)
        # if table is anything other than prospects or teams, don't allow edit
        else:
            bit = 2
            table_error = DBFunctions.show_error(bit)
            if table_error > 0:  # quit function if you get here
                return
        # reload table
        DBFunctions.load_table(self)

    def delete_entry(self):
        # get table name
        table_name = self.select_table_list.currentText()
        # if table is prospects go ahead and edit
        if table_name == "prospects":
            # get select rows' contents
            data = []
            for index in self.view_database_table.selectionModel().selectedRows():
                row = index.row()
                for column in range(8):  # Can only edit prospects tables which has 8 columns
                    if self.view_database_table.item(row, column) is None:
                        # This will skip empty columns if nothing selected
                        continue
                    else:
                        data.append(self.view_database_table.item(row, column).text())
            # if there is nothing in data, row was not selected, show error message
            if len(data) == 0:
                bit = 1
                table_error = DBFunctions.show_error(bit)
                if table_error > 0:  # quit function if you get here
                    return
            # connect to database
            db_conn = db_connection()
            connection = db_conn[0]
            cursor = db_conn[1]
            # delete selected row
            delete_key = data[0]
            sql = "DELETE FROM prospects WHERE Name=%s"
            val = (delete_key,)
            cursor.execute(sql, val)
            connection.commit()
            close_db_connection(connection)
        # if table is anything other than prospects or teams, don't allow edit
        else:
            bit = 3
            table_error = DBFunctions.show_error(bit)
            if table_error > 0:  # quit function if you get here
                return
        # reload table
        DBFunctions.load_table(self)
