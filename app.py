from PyQt5.QtWidgets import QMainWindow
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import pyqtSignal, QThread, QObject
# import GUI from designer file
from gui import Ui_MainWindow
# import other functions
from data_grab import UiSetup, SeasonData, SeasonScrape, PlayerSeasonData
from database_funct import UiDBsetup, DBFunctions


class Main(QMainWindow, Ui_MainWindow):
    def __init__(self, parent=None):
        super(Main, self).__init__(parent)
        # Create UI
        self.setupUi(self)

        # DATA GRAB SECTION
        # Populate data grab Combo Boxes
        UiSetup.populate_lists(self)
        # Connect combobox to function that will change player list based on team list selection
        self.team_select_combobox.currentTextChanged.connect(lambda: UiSetup.change_prospect_list(self))
        # Get player season data when button is clicked
        self.get_player_data_button.clicked.connect(lambda: SeasonScrape.call_scrape_thread(self))
        # Clear Table Button
        self.clear_table_button.clicked.connect(lambda: UiSetup.clear_table(self))
        # Get player season data
        self.get_season_data_button.clicked.connect(lambda: PlayerSeasonData.get_player_data(self))

        # DATABASE VIEW SECTION
        # Populate database view combobox
        UiDBsetup.pop_table_list(self)
        # Load table data
        self.load_table_button.clicked.connect(lambda: DBFunctions.load_table(self))

    # Add Season data to table once scrape is complete [Called from SeasonScrape, call_scrape_thread]
    @QtCore.pyqtSlot(list)
    def add_to_table(self, season_data):
        year = season_data[0]
        team = season_data[1]
        league = season_data[2]
        gp = season_data[3]
        for row in range(len(year)):
            row_position = self.tableWidget.rowCount()
            self.tableWidget.insertRow(row_position)
            for col in range(len(season_data)):
                if col == 0:
                    self.tableWidget.setItem(row_position, col, QtWidgets.QTableWidgetItem(str(year[row])))
                if col == 1:
                    self.tableWidget.setItem(row_position, col, QtWidgets.QTableWidgetItem(str(team[row])))
                if col == 2:
                    self.tableWidget.setItem(row_position, col, QtWidgets.QTableWidgetItem(str(league[row])))
                if col == 3:
                    self.tableWidget.setItem(row_position, col, QtWidgets.QTableWidgetItem(str(gp[row])))
        self.tableWidget.resizeColumnsToContents()


if __name__ == "__main__":
    import sys

    app = QtWidgets.QApplication(sys.argv)
    main_menu = Main()
    main_menu.show()
    sys.exit(app.exec_())
