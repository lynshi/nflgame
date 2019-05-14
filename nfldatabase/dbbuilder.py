from nfldatabase.database import NFLDatabase

import nflgame


class NFLdbBuilder:
    def __init__(self, db_file_name):
        """
        Build a SQLite3 database, for existing NFL data,
        in the file found at db_file_name.

        :param db_file_name: name of file to store database in
        """
        self.db = NFLDatabase(db_file_name)
        self.db.create_players_table()
        self.db.create_teams_table()
        self.db.create_games_table()

    def run(self):
        """
        Run data insertion process. Teams are inserted first due to foreign key
        constraints, then players and games.

        :return: NFLDatabase instance
        """
        self._insert_teams()

        return self.db

    def _insert_teams(self):
        """
        Insert all teams found in nflgame.teams into database.

        :return: None
        """
        self.db.insert_teams(nflgame.teams)
