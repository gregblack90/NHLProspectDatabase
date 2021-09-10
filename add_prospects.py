from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from datetime import date
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


class UiAPsetup:
    def pop_combobox_lists(self):
        # Team ComboBox
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        cursor.execute("SELECT * FROM teams")
        result = cursor.fetchall()
        teams = []
        for x in result:
            teams.append(x[0])
        close_db_connection(connection)
        self.team_entry_comboBox.addItems(teams)
        # Position ComboBox
        positions = ["F", "D", "G"]
        self.position_select_comboBox.addItems(positions)
        # Height ComboBox
        feet = ["4", "5", "6", "7", "8"]
        inches = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']
        self.height_feet_comboBox.addItems(feet)
        self.height_in_comboBox.addItems(inches)
        # DOB
        month = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        day = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12',
               '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
               '25', '26', '27', '28', '29', '30', '31']
            # get current year
        todays_date = date.today()
        current_year = todays_date.year
        year = []
        for x in range(current_year-25, current_year+1):
            year.append(str(x))
        self.dob_m_comboBox.addItems(month)
        self.dob_d_comboBox.addItems(day)
        self.dob_y_comboBox.addItems(year)


class AddProspectFunctions:
    def success_pop_up(self):
        msg = QMessageBox()
        msg.setText("Entry Successful!")
        msg.setWindowTitle("Success!")
        msg.setStandardButtons(QMessageBox.Ok)
        return msg.exec_()

    def user_info_check_pop_up(self, player_name, team, position, height, weight, dob, birthplace):
        msg = QMessageBox()
        msg.setText("Name: " + player_name + "\nTeam: " + team + "\nPosition: " + position + "\nHeight: " + height + "\nWeight: " + weight +
                    "\nDOB: " + dob + "\nBirthplace " + birthplace)
        msg.setWindowTitle("Check Information")
        msg.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        return msg.exec_()

    def add_prospect(self):
        # Get information from GUI
        player_name = self.player_name_lineEdit.text()
        team = self.team_entry_comboBox.currentText()
        position = self.position_select_comboBox.currentText()
        height = self.height_feet_comboBox.currentText() + "' " + self.height_in_comboBox.currentText() + '"'
        weight = self.enter_weight_lineEdit.text()
        dob = self.dob_m_comboBox.currentText() + "/" + self.dob_d_comboBox.currentText() + "/" + self.dob_y_comboBox.currentText()
        birthplace = self.enter_birthplace_lineEdit.text()
        # Check information with user
        reply = AddProspectFunctions.user_info_check_pop_up(self, player_name, team, position, height, weight, dob, birthplace)
        # If user clicks cancel or closes popup window, exit function
        if reply == QMessageBox.Cancel:
            return
        # Otherwise, connect to DB
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # Insert Data
        sql = "INSERT INTO prospects (Name, Team, Position, Height, Weight, DOB, BirthPlace)" \
              "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (player_name, team, position, height, weight, dob, birthplace)
        cursor.execute(sql, val)
        connection.commit()
        # Close DB
        close_db_connection(connection)
        # Alert User
        AddProspectFunctions.success_pop_up(self)
        # Clear information
        self.player_name_lineEdit.clear()
        self.enter_weight_lineEdit.clear()
        self.enter_birthplace_lineEdit.clear()

    def get_file_path(self):
        file_path = QFileDialog.getOpenFileName(self, 'Open file')
        self.file_path_readonly_lineEdit.setText(file_path[0])

    def upload_from_file(self):
        # Get file path from text box
        file_path = self.file_path_readonly_lineEdit.text()
        prospect_list_file = open(file_path, 'r')
        # connect to database
        db_conn = db_connection()
        connection = db_conn[0]
        cursor = db_conn[1]
        # read data from text file, insert into database
        for line in prospect_list_file:
            text = line.rstrip('\n')
            currentLine = text.split(",")
            Name = currentLine[0].title()
            Team = currentLine[1]
            Position = currentLine[2]
            Height = currentLine[3]
            Weight = currentLine[4]
            DOB = currentLine[5]
            BirthPlace = currentLine[6].title()
            sql = "INSERT INTO prospects (Name, Team, Position, Height, Weight, DOB, Birthplace)" \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (Name, Team, Position, Height, Weight, DOB, BirthPlace)
            cursor.execute(sql, val)
            connection.commit()
        # close database connection
        close_db_connection(connection)
