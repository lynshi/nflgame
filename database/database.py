import sqlite3 as sql


class NFLDatabase:
    def __init__(self, db_file_name):
        """
        Build a SQLite3 database in the file found at db_file_name
        :param db_file_name: name of file to store database in
        """
        self.db_file_name = db_file_name
        self.conn = sql.connect(self.db_file_name)
        self.cursor = self.conn.cursor()

    def close(self):
        """
        Call to explicitly close db connection

        :return: None
        """
        self.conn.close()

    def __del__(self):
        """
        Close Connection instance

        :return: None
        """
        self.close()
