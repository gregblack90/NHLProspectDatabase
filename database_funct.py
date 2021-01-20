from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
import mysql.connector as mysql
import PTEdbConfig as guiConfig


# Open Database connection
#   --> return connection and cursor
def db_connection():
    conn = mysql.connect(**guiConfig.dbConfig)
    curs = conn.cursor()
    return[conn, curs]


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
            res = str(x[0].title())
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
        if table_name == "Teams":
            # Select all data from teams table
            cursor.execute("SELECT * FROM teams")
            result = cursor.fetchall()
            # append results to list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Team", "Conference", "Division", "", "", "", "", ""]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
            # for each row...
            for row in range(len(data)):
                # get number of total rows...
                row_position = self.view_database_table.rowCount()
                # insert new row...
                self.view_database_table.insertRow(row_position)
                # for each column in new row, insert data into columns
                for col in range(len(data[row_position])):
                    if col == 0:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 1:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 2:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
        # PROSPECTS TABLE
        if table_name == "Prospects":
            # Select all data from teams table
            cursor.execute("SELECT * FROM prospects")
            result = cursor.fetchall()
            # append results to list
            for x in result:
                data.append(x)
            # set headers
            header_labels = ["Name", "Team", "Position", "Height", "Weight", "DOB", "Age", "Birthplace"]
            self.view_database_table.setHorizontalHeaderLabels(header_labels)
            # for each row...
            for row in range(len(data)):
                # get number of total rows...
                row_position = self.view_database_table.rowCount()
                # insert new row...
                self.view_database_table.insertRow(row_position)
                # for each column in new row, insert data into columns
                for col in range(len(data[row_position])):
                    if col == 0:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 1:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 2:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 3:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 4:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 5:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 6:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
                    if col == 7:
                        self.view_database_table.setItem(row_position, col, QtWidgets.QTableWidgetItem(data[row_position][col]))
        # resize columns to fit data
        self.view_database_table.resizeColumnsToContents()
        # close database connection
        close_db_connection(connection)
