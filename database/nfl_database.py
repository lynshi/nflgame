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

    def commit(self):
        """
        Commit database

        :return: None
        """
        self.conn.commit()

    def create_players_table(self):
        self.cursor.execute("""
            CREATE TABLE Players (
                player_id CHAR(10) PRIMARY KEY NOT NULL, 
                gsis_name VARCHAR(50) NOT NULL, 
                full_name VARCHAR(50) NOT NULL, 
                first_name VARCHAR(50) NOT NULL, 
                last_name VARCHAR(50) NOT NULL,
                team VARCHAR(3) NOT NULL, 
                position VARCHAR(6) NOT NULL, 
                profile_id INT NOT NULL, 
                profile_url VARCHAR(100) NOT NULL, 
                uniform_number INT NOT NULL,
                birthdate VARCHAR(10) NOT NULL, 
                college VARCHAR(50) NOT NULL, 
                height INT NOT NULL, 
                weight INT NOT NULL, 
                years_pro INT NOT NULL, 
                status VARCHAR(10) NOT NULL
            )
        """,)
        self.commit()
