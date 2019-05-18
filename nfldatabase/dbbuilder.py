from nfldatabase.database import NFLDatabase

from collections import Counter
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
        self.db.create_player_game_statistics_table()
        self.db.create_team_game_statistics_table()

    @staticmethod
    def _find_stat_columns():
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

    def run(self):
        """
        Run data insertion process. Teams are inserted first due to foreign key
        constraints, then players and games.

        :return: NFLDatabase instance
        """

        self._insert_teams()
        self._insert_games()
        self._insert_players_and_game_statistics()

        return self.db

    def _insert_teams(self):
        """
        Insert all teams found in nflgame.teams into database.

        :return: None
        """
        self.db.insert_teams(nflgame.teams)

    def _insert_games(self):
        """
        Insert all games from 2009 to present.

        :return: None
        """
        self.db.insert_games(nflgame.sched.games)

    def _insert_players_and_game_statistics(self):
        """
        Insert all players present from 2009-present. In addition, insert game
        statistics for all players and teams in games in all phases from 2009
        to present.

        :return: None
        """

        stat_columns = set()
        phases = [('PRE', 4), ('REG', 17), ('POST', 4)]
        seasons = [i for i in range(2009, datetime.datetime.now().year)]

        inserted_player_ids = set()

        # I acknowledge this is terribly ugly.
        # This can potentially be multi-threaded, but I figured the speedup is
        # kind of small due to the bottleneck of writing to the db.
        for season in seasons:
            for phase, num_weeks in phases:
                for week in range(1, num_weeks + 1):
                    try:
                        games = nflgame.games(year=season, week=week,
                                              kind=phase)
                    except TypeError:
                        continue

                    for game in games:
                        players = nflgame.combine_game_stats([game])
                        team_stats = {
                            game.home: Counter({}),
                            game.away: Counter({})
                        }

                        for p in players:
                            if p.playerid not in inserted_player_ids:
                                self.db.insert_players(
                                    nflgame.players[p.playerid])

                            self.db.insert_player_game_statistics(p.playerid,
                                                                  game.eid,
                                                                  p._stats)
                            team_stats[p.team] += Counter(p._stats)

                        for team, stats in team_stats.items():
                            self.db.insert_team_game_statistics(team, game.eid,
                                                                stats)

        return stat_columns
