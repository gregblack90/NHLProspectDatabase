from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
import mysql.connector as mysql
import dbConfig as guiConfig


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
    def load_table(self):
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
        # PROSPECT STATS
        elif table_name != "teams" or table_name != "prospects":
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
            # for each column in new row, insert data into columns
            for col in range(len(data[row_position])):
                if col == 0:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(str(data[row_position][col])))
                if col == 1:
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
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
                    self.view_database_table.setItem(row_position, col,
                                                     QtWidgets.QTableWidgetItem(data[row_position][col]))
                if col == 7:
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

        # function for error message if user tries to edit a table that is not prospects or teams
        def show_error(error_bit):
            def close_error_msg():
                error_msg.close()
                return
            error_msg = QtWidgets.QMessageBox()
            error_msg.setIcon(QtWidgets.QMessageBox.Warning)
            if error_bit == 1:
                error_msg.setText("Please select a row")
            elif error_bit == 2:
                error_msg.setText("Cannot edit tables other than 'prospects' or 'teams'")
            error_msg.setWindowTitle("ERROR")
            error_msg.setStandardButtons(QtWidgets.QMessageBox.Ok)
            error_msg.buttonClicked.connect(close_error_msg)
            return error_msg.exec_()

        # if table is prospects or teams, go ahead and edit
        if table_name == "prospects" or table_name == "teams":
            # get select rows' contents
            data = []
            for index in self.view_database_table.selectionModel().selectedRows():
                row = index.row()
                for column in range(8):  # Can only edit teams/prospects tables which have a max of 8 columns
                    if self.view_database_table.item(row, column) is None:  # This will skip empty columns in team table
                        continue
                    else:
                        data.append(self.view_database_table.item(row, column).text())
            # if there is nothing in data, row was not selected, show error message
            if len(data) == 0:
                bit = 1
                table_error = show_error(bit)
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
                                               "Age=%s, Birthplace=%s WHERE Name=%s"
                val = (team, pos, height, weight, dob, age, bp, name)
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

        # if table is anything other than prospects or teams, don't allow edit
        else:
            bit = 2
            table_error = show_error(bit)
            if table_error > 0:  # quit function if you get here
                return

