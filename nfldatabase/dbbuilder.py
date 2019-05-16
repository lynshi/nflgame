from nfldatabase.database import NFLDatabase

import datetime
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

        stat_columns = self._find_stat_columns()
        self.db.create_player_game_statistics_table(stat_columns)
        self.db.create_team_game_statistics_table(stat_columns)

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

    def _find_stat_columns(self):
        """
        Find all statistic names for players in nflgame.

        :return: list of set of statistic names
        """

        stat_columns = set()
        phases = [('PRE', 4), ('REG', 17), ('POST', 4)]
        seasons = [i for i in range(2009, datetime.datetime.now().year)]

        for season in seasons:
            for phase, num_weeks in phases:
                for week in range(1, num_weeks + 1):
                    try:
                        games = nflgame.games(year=season, week=week,
                                              kind=phase)
                    except TypeError:
                        continue

                    players = nflgame.combine_game_stats(games)
                    for player in players:
                        for stat in player._stats:
                            stat_columns.add(stat)

        return stat_columns
