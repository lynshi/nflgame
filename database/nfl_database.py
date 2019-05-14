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
        """
        Create Players table to store player information

        :return: None
        """
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

    def insert_players(self, players):
        """
        Insert player data from players into the Players table. If players is a
        single item, it is converted to a list automatically.

        :param players: list of Player objects whose data to insert
        :return: None
        """
        if isinstance(players, list) is False \
                and isinstance(players, tuple) is False:
            players = [players]

        query = """INSERT INTO Players Values """
        attributes = [
            'player_id', 'gsis_name', 'full_name', 'first_name',
            'last_name', 'team', 'position', 'profile_id', 'profile_url',
            'uniform_number', 'birthdate', 'college', 'height', 'weight',
            'years_pro', 'status'
        ]

        params = []
        for p in players:
            for a in attributes:
                params.append(getattr(p, a))
        params = tuple(params)

        row_placeholder = '(' + '?,' * (len(attributes) - 1) + '?), '
        query += row_placeholder * len(players)

        self.cursor.execute(query[:-2], params)
        self.commit()
