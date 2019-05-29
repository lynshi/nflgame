from nfldatabase.database import NFLDatabase

from collections import Counter
import datetime
import nflgame
import os


def find_stat_columns():
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

                players = nflgame.combine_play_stats(games)
                for player in players:
                    for stat in player._stats:
                        stat_columns.add(stat)

    return stat_columns


class NFLdbBuilder:
    def __init__(self, db_file_name=None):
        """
        Build a SQLite3 database, for existing NFL data,
        in the file found at db_file_name.

        :param db_file_name: name of file to store database in
        """
        self._is_new_db = False
        if db_file_name is None:
            db_file_name = os.path.join(os.path.dirname(__file__), 'nfl.db')

        if os.path.isfile(db_file_name) is False:
            self._is_new_db = True

        self.db = NFLDatabase(db_file_name)
        if self._is_new_db is True:
            self._create_tables()

    def _create_tables(self):
        """
        Create all necessary tables in NFLDatabase

        :return: None
        """
        self.db.create_players_table()
        self.db.create_teams_table()
        self.db.create_games_table()
        self.db.create_player_game_statistics_table()
        self.db.create_team_game_statistics_table()

    def run(self, reset=False, update=False):
        """
        Run data insertion process. Teams are inserted first due to foreign key
        constraints, then players and games.

        :param reset: If True, drop all tables and repopulate. Else,
            only add new game statistics starting from the last game in the
            database.
        :param update: If True, only inserts new data into database per
            specifications in self._insert_game_statistics(). Ignored if reset
            is True.
        :return: NFLDatabase instance
        """

        if reset is True:
            self.db.reset()
            self._is_new_db = True

        if self._is_new_db is True:
            for func in [self._insert_teams, self._insert_games,
                         self._insert_players]:
                func()

        if self._is_new_db is True:
            self._insert_game_statistics(update=False)
        else:
            self._insert_game_statistics(update)

        self._is_new_db = False

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

    def _insert_players(self):
        """
        Insert all player information.

        :return: None
        """
        self.db.insert_players(nflgame.players.values())

    def _insert_game_statistics(self, update):
        """
        Insert game statistics for all players and teams in games in all phases.
        Players with ids not found in nflgame.players are ignored.

        :param update: If True, only add games not completely present in the
            database. Completeness is checked by ensuring both teams in the game
            have team statistics recorded. Else, add all games starting in 2009.
        :return: None
        """

        phases = [('PRE', 4), ('REG', 17), ('POST', 4)]
        seasons = [i for i in range(2009, datetime.datetime.now().year)]

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
                        if update is True:
                            res = self.db.cursor.execute("SELECT * FROM "
                                                         "Team_Game_Statistics "
                                                         "WHERE eid = ?",
                                                         (game.eid,)).fetchall()
                            if len(res) == 2:
                                continue
                            elif len(res) == 1:
                                self.db.cursor.execute("DELETE FROM "
                                                       "Team_Game_Statistics "
                                                       "WHERE eid = ?",
                                                       (game.eid,))
                                self.db.cursor.execute("DELETE FROM "
                                                       "Player_Game_Statistics "
                                                       "WHERE eid = ?",
                                                       (game.eid,))

                        players = nflgame.combine_play_stats([game])
                        team_stats = {
                            game.home: Counter({}),
                            game.away: Counter({})
                        }

                        for p in players:
                            if p.playerid not in nflgame.players:
                                continue

                            self.db.insert_player_game_statistics(p.playerid,
                                                                  game.eid,
                                                                  p._stats)
                            team_stats[p.team] += Counter(p._stats)

                        for team, stats in team_stats.items():
                            self.db.insert_team_game_statistics(team, game.eid,
                                                                stats)