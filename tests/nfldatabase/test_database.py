from collections import OrderedDict
import sqlite3 as sql
import unittest

import nflgame
from nflgame.player import Player
import nfldatabase.database as nfldb
from nfldatabase.dbbuilder import find_stat_columns


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = nfldb.NFLDatabase(':memory:')
        self.db.create_players_table()
        self.db.create_teams_table()
        self.db.create_games_table()
        self.db.create_player_game_statistics_table()
        self.db.create_team_game_statistics_table()

        self.stat_columns = {
            'punting_touchback', 'receiving_tds', 'punting_blk',
            'defense_sk_yds', 'kicking_i20', 'kicking_all_yds',
            'defense_tkl', 'rushing_tds', 'rushing_yds',
            'receiving_tar', 'passing_cmp', 'kicking_rec',
            'kicking_fgmissed', 'kicking_fgm_yds',
            'fumbles_notforced', 'kicking_touchback',
            'fumbles_lost', 'defense_pass_def',
            'passing_twoptm', 'kicking_xpmade', 'kickret_ret',
            'fumbles_forced', 'fumbles_rec', 'kicking_xpb',
            'punting_tot', 'punting_i20',
            'defense_tkl_loss_yds', 'defense_frec',
            'passing_incmp', 'defense_frec_tds',
            'fumbles_rec_tds', 'passing_incmp_air_yds',
            'fumbles_rec_yds', 'defense_ffum', 'puntret_yds',
            'receiving_rec', 'rushing_att', 'passing_att',
            'kicking_fgm', 'kicking_rec_tds', 'kicking_yds',
            'rushing_twoptm', 'defense_tkl_primary',
            'passing_twopta', 'defense_frec_yds',
            'defense_int_tds', 'defense_xpblk', 'defense_fgblk',
            'kicking_tot', 'defense_int_yds',
            'kicking_fgmissed_yds', 'defense_safe',
            'receiving_twoptm', 'passing_twoptmissed',
            'passing_int', 'passing_cmp_air_yds',
            'receiving_twopta', 'defense_qbhit',
            'kicking_xpmissed', 'passing_yds', 'passing_tds',
            'kicking_fga', 'defense_puntblk', 'kickret_yds',
            'defense_ast', 'receiving_twoptmissed',
            'punting_yds', 'defense_tds', 'passing_sk_yds',
            'kickret_tds', 'rushing_twopta', 'fumbles_oob',
            'fumbles_tot', 'defense_misc_tds',
            'defense_tkl_loss', 'defense_misc_yds',
            'passing_sk', 'defense_int', 'puntret_fair',
            'kickret_fair', 'receiving_yds',
            'rushing_twoptmissed', 'puntret_tds', 'kicking_xpa',
            'kicking_fgb', 'defense_sk', 'puntret_tot',
            'penalty_yds', 'penalty', 'receiving_yac_yds'
        }

    def test_valid_stat_columns(self):
        self.assertSetEqual(self.db._valid_stat_columns, find_stat_columns())

    def test_close(self):
        self.db.close()
        self.assertRaises(sql.ProgrammingError, self.db.cursor.execute,
                          'CREATE TABLE Failure')

    def test_drop_table(self):
        valid_tables = {'Players', 'Games', 'Teams', 'Player_Game_Statistics',
                        'Team_Game_Statistics'}
        for table in valid_tables:
            # Make sure no exception here
            self.db.cursor.execute("SELECT * FROM " + table)
            self.db._drop_table(table)
            self.assertRaises(sql.OperationalError, self.db.cursor.execute,
                              "SELECT * FROM " + table)

    def test_drop_invalid_table(self):
        self.assertRaises(RuntimeError, self.db._drop_table, 'LOLno')

    def test_reset(self):
        self.db.reset()
        valid_tables = {'Players', 'Games', 'Teams', 'Player_Game_Statistics',
                        'Team_Game_Statistics'}
        for table in valid_tables:
            self.assertRaises(sql.OperationalError, self.db.cursor.execute,
                              "SELECT * FROM " + table)

    def test_reset_with_data(self):
        self.db.insert_teams(nflgame.teams)
        self.db.insert_games(nflgame.sched.games)
        self.db.insert_players(nflgame.players.values())

        games = nflgame.games(year=2011, week=16,
                              kind='REG')

        for game in games:
            players = nflgame.combine_play_stats([game])
            for p in players:
                self.db.insert_player_game_statistics(p.playerid,
                                                      game.eid,
                                                      p._stats)

        self.db.reset()
        valid_tables = {'Players', 'Games', 'Teams', 'Player_Game_Statistics',
                        'Team_Game_Statistics'}
        for table in valid_tables:
            self.assertRaises(sql.OperationalError, self.db.cursor.execute,
                              "SELECT * FROM " + table)

    def test_get_table_column_names(self):
        tables = {
            'Players': {
                'player_id', 'gsis_name', 'full_name', 'first_name',
                'last_name', 'team', 'position', 'profile_id', 'profile_url',
                'uniform_number', 'birthdate', 'college', 'height', 'weight',
                'years_pro', 'status'
            },
            'Teams': {'team', 'city', 'team_name', 'full_name', 'alt_abbrev'},
            'Player_Game_Statistics': self.stat_columns.union({
                'player_id', 'eid'
            }),
            'Team_Game_Statistics': self.stat_columns.union({
                'team', 'eid'
            }),
            'Games': {
                'away', 'day', 'eid', 'gamekey', 'home', 'season_type', 'time',
                'meridiem', 'wday', 'week', 'year'
            }
        }
        for t_name, t_contents in tables.items():
            columns = set(self.db.get_table_column_names(t_name))
            self.assertSetEqual(columns, t_contents)

    def test_get_table_column_names_invalid_column(self):
        self.assertRaises(RuntimeError, self.db.get_table_column_names,
                          'Not_A_Statistic')

    def test_players_table_creation(self):
        res = self.db.cursor.execute('PRAGMA table_info(Players)').fetchall()
        expected_columns = {
            'player_id': ('CHAR(10)', 1, None, 1),
            'gsis_name': ('VARCHAR(50)', 1, None, 0),
            'full_name': ('VARCHAR(50)', 1, None, 0),
            'first_name': ('VARCHAR(50)', 1, None, 0),
            'last_name': ('VARCHAR(50)', 1, None, 0),
            'team': ('VARCHAR(3)', 1, None, 0),
            'position': ('VARCHAR(6)', 1, None, 0),
            'profile_id': ('INT', 1, None, 0),
            'profile_url': ('VARCHAR(100)', 1, None, 0),
            'uniform_number': ('INT', 1, None, 0),
            'birthdate': ('VARCHAR(10)', 1, None, 0),
            'college': ('VARCHAR(50)', 1, None, 0),
            'height': ('REAL', 1, None, 0),
            'weight': ('REAL', 1, None, 0),
            'years_pro': ('INT', 1, None, 0),
            'status': ('VARCHAR(10)', 1, None, 0)
        }

        for col in res:
            column = col[1]
            self.assertIn(column, expected_columns)
            self.assertTupleEqual(expected_columns[column], col[2:])
            del expected_columns[column]

        self.assertEqual(len(expected_columns), 0)

    def test_teams_table_creation(self):
        res = self.db.cursor.execute('PRAGMA table_info(Teams)').fetchall()
        expected_columns = {
            'team': ('VARCHAR(3)', 1, None, 1),
            'city': ('VARCHAR(50)', 1, None, 0),
            'team_name': ('VARCHAR(50)', 1, None, 0),
            'full_name': ('VARCHAR(50)', 1, None, 0),
            'alt_abbrev': ('VARCHAR(3)', 0, None, 0)
        }

        for col in res:
            column = col[1]
            self.assertIn(column, expected_columns)
            self.assertTupleEqual(expected_columns[column], col[2:])
            del expected_columns[column]

        self.assertEqual(len(expected_columns), 0)

    def test_games_table_creation(self):
        res = self.db.cursor.execute('PRAGMA table_info(Games)').fetchall()

        expected_columns = {
            'away': ('VARCHAR(3)', 1, None, 0),
            'day': ('INT', 1, None, 0),
            'eid': ('VARCHAR(10)', 1, None, 1),
            'gamekey': ('VARCHAR(10)', 1, None, 0),
            'home': ('VARCHAR(3)', 1, None, 0),
            'season_type': ('VARCHAR(4)', 1, None, 0),
            'time': ('VARCHAR(5)', 1, None, 0),
            'meridiem': ('CHAR(2)', 0, None, 0),
            'wday': ('CHAR(3)', 1, None, 0),
            'week': ('INT', 1, None, 0),
            'year': ('INT', 1, None, 0)
        }

        for col in res:
            column = col[1]
            self.assertIn(column, expected_columns)
            self.assertTupleEqual(expected_columns[column], col[2:])
            del expected_columns[column]

        self.assertEqual(len(expected_columns), 0)

    def test_home_foreign_key_constraint_in_games(self):
        game = {
            "2015102500": {
                "away": "BUF",
                "day": 25,
                "eid": "2015102500",
                "gamekey": "56595",
                "home": "JAC",
                "meridiem": "AM",
                "month": 10,
                "season_type": "REG",
                "time": "9:30",
                "wday": "Sun",
                "week": 7,
                "year": 2015
            }
        }

        self.db.insert_teams(['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'])
        self.assertRaises(sql.IntegrityError, self.db.insert_games, game)

    def test_away_foreign_key_constraint_in_games(self):
        game = {
            "2015102500": {
                "away": "BUF",
                "day": 25,
                "eid": "2015102500",
                "gamekey": "56595",
                "home": "JAC",
                "meridiem": "AM",
                "month": 10,
                "season_type": "REG",
                "time": "9:30",
                "wday": "Sun",
                "week": 7,
                "year": 2015
            }
        }

        self.db.insert_teams(
            ['JAC', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars'])
        self.assertRaises(sql.IntegrityError, self.db.insert_games, game)

    def test_player_game_statistics_table_creation(self):
        res = self.db.cursor.execute('PRAGMA table_info('
                                     'Player_Game_Statistics)').fetchall()

        expected_columns = {
            'eid': ('VARCHAR(10)', 1, None, 2),
            'player_id': ('CHAR(10)', 1, None, 1)
        }
        tup = ('REAL', 0, '0', 0)
        stat_cols = self.stat_columns
        for s in stat_cols:
            expected_columns[s] = tup

        for col in res:
            column = col[1]
            self.assertIn(column, expected_columns)
            self.assertTupleEqual(expected_columns[column], col[2:])
            del expected_columns[column]

        self.assertEqual(len(expected_columns), 0)

    def test_player_game_statistics_eid_foreign_key_constraint(self):
        self.assertRaises(sql.IntegrityError,
                          self.db.insert_player_game_statistics,
                          '1234567890', '2015102500', {'defense_ast': 23})

    def test_team_game_statistics_table_creation(self):
        res = self.db.cursor.execute('PRAGMA table_info('
                                     'Team_Game_Statistics)').fetchall()

        expected_columns = {
            'eid': ('VARCHAR(10)', 1, None, 2),
            'team': ('VARCHAR(3)', 1, None, 1)
        }
        tup = ('REAL', 0, '0', 0)
        stat_cols = self.stat_columns
        for s in stat_cols:
            expected_columns[s] = tup

        for col in res:
            column = col[1]
            self.assertIn(column, expected_columns)
            self.assertTupleEqual(expected_columns[column], col[2:])
            del expected_columns[column]

        self.assertEqual(len(expected_columns), 0)

    def test_team_game_statistics_eid_foreign_key_constraint(self):
        self.assertRaises(sql.IntegrityError,
                          self.db.insert_team_game_statistics,
                          'NYG', '2015102500', {'defense_ast': 23})

    def test_insert_players(self):
        test_set = {
            "00-0020531": {
                "birthdate": "1/15/1979",
                "college": "Purdue",
                "first_name": "Drew",
                "full_name": "Drew Brees",
                "gsis_id": "00-0020531",
                "gsis_name": "D.Brees",
                "height": 72,
                "last_name": "Brees",
                "number": 9,
                "position": "QB",
                "profile_id": 2504775,
                "profile_url": "http://www.nfl.com/player/drewbrees/2504775/profile",
                "status": "ACT",
                "team": "NO",
                "weight": 209,
                "years_pro": 19
            },
            "00-0020536": {
                "birthdate": "6/23/1979",
                "college": "Texas Christian",
                "first_name": "LaDainian",
                "full_name": "LaDainian Tomlinson",
                "gsis_id": "00-0020536",
                "gsis_name": "L.Tomlinson",
                "height": 70,
                "last_name": "Tomlinson",
                "profile_id": 2504778,
                "profile_url": "http://www.nfl.com/player/ladainiantomlinson/2504778/profile",
                "weight": 215,
                "years_pro": 11
            },
            "00-0020543": {
                "birthdate": "6/23/1978",
                "college": "Purdue",
                "first_name": "Matt",
                "full_name": "Matt Light",
                "gsis_id": "00-0020543",
                "gsis_name": "M.Light",
                "height": 76,
                "last_name": "Light",
                "profile_id": 2504784,
                "profile_url": "http://www.nfl.com/player/mattlight/2504784/profile",
                "weight": 305,
                "years_pro": 11
            },
            "00-0027265": {
                "birthdate": "11/11/1986",
                "college": "Massachusetts",
                "first_name": "Victor",
                "full_name": "Victor Cruz",
                "gsis_id": "00-0027265",
                "gsis_name": "V.Cruz",
                "height": 72,
                "last_name": "Cruz",
                "number": 80,
                "profile_id": 2507855,
                "profile_url": "http://www.nfl.com/player/victorcruz/2507855/profile",
                "weight": 202,
                "years_pro": 8
            },
            "00-0034844": {
                "birthdate": "2/9/1997",
                "college": "Penn State",
                "first_name": "Saquon",
                "full_name": "Saquon Barkley",
                "gsis_id": "00-0034844",
                "height": 72,
                "last_name": "Barkley",
                "number": 26,
                "position": "RB",
                "profile_id": 2559901,
                "profile_url": "http://www.nfl.com/player/saquonbarkley/2559901/profile",
                "status": "ACT",
                "team": "NYG",
                "weight": 233,
                "years_pro": 2
            },
            "00-0034845": {
                "birthdate": "2/17/1995",
                "college": "Georgia",
                "first_name": "Sony",
                "full_name": "Sony Michel",
                "gsis_id": "00-0034845",
                "height": 71,
                "last_name": "Michel",
                "number": 26,
                "position": "RB",
                "profile_id": 2559842,
                "profile_url": "http://www.nfl.com/player/sonymichel/2559842/profile",
                "status": "ACT",
                "team": "NE",
                "weight": 215,
                "years_pro": 2
            }
        }
        players = {}
        for pid in test_set:
            players[pid] = Player(test_set[pid])

        columns = self.db.get_table_column_names('Players')

        self.db.insert_players(players.values())
        inserted = self.db.cursor.execute("SELECT * FROM Players")

        for res in inserted:
            self.assertIn(res[0], test_set)
            player = players[res[0]]
            for idx, name in enumerate(columns):
                self.assertEqual(res[idx], getattr(player, name))

    def test_insert_more_than_999_players(self):
        info = {
            "birthdate": "2/17/1995",
            "college": "Georgia",
            "first_name": "Sony",
            "full_name": "Sony Michel",
            "height": 71,
            "last_name": "Michel",
            "number": 26,
            "position": "RB",
            "profile_id": 2559842,
            "profile_url": "http://www.nfl.com/player/sonymichel/2559842/profile",
            "status": "ACT",
            "team": "NE",
            "weight": 215,
            "years_pro": 2
        }

        players = {}
        for i in range(999*3 + 500):
            i = str(i).zfill(10)
            p = {**info, **{'gsis_id': i}}
            players[i] = Player(p)

        columns = self.db.get_table_column_names('Players')

        self.db.insert_players(players.values())
        inserted = self.db.cursor.execute("SELECT * FROM Players")

        for res in inserted:
            self.assertIn(res[0], players)
            player = players[res[0]]
            for idx, name in enumerate(columns):
                self.assertEqual(res[idx], getattr(player, name))

    def test_insert_one_player(self):
        data = {
            "birthdate": "2/9/1997",
            "college": "Penn State",
            "first_name": "Saquon",
            "full_name": "Saquon Barkley",
            "gsis_id": "00-0034844",
            "height": 72,
            "last_name": "Barkley",
            "number": 26,
            "position": "RB",
            "profile_id": 2559901,
            "profile_url": "http://www.nfl.com/player/saquonbarkley/2559901/profile",
            "status": "ACT",
            "team": "NYG",
            "weight": 233,
            "years_pro": 2
        }
        player = Player(data)

        columns = self.db.get_table_column_names('Players')

        self.db.insert_players(player)
        inserted = self.db.cursor.execute("SELECT * FROM Players").fetchall()

        self.assertEqual(len(inserted), 1)
        res = inserted[0]
        for idx, name in enumerate(columns):
            self.assertEqual(res[idx], getattr(player, name))

    def test_insert_teams(self):
        self.db.insert_teams(nflgame.teams)
        res = self.db.cursor.execute("SELECT * FROM Teams").fetchall()

        self.assertEqual(len(res), len(nflgame.teams))
        for row in res:
            found = False

            for team in nflgame.teams:
                if team[0] == row[0]:
                    found = [team[0]] if len(team) == 4 else []
                    self.assertListEqual(list(row), team + found)
                    found = True
                    break

            self.assertTrue(found)

    def test_insert_one_team(self):
        self.db.insert_teams(['NYG', 'New York G', 'Giants', 'New York Giants'])
        res = self.db.cursor.execute("SELECT * FROM Teams").fetchall()

        self.assertEqual(len(res), 1)
        res = res[0]

        found = False
        for team in nflgame.teams:
            if team[0] == res[0]:
                found = True
                self.assertListEqual(list(res), team + [team[0]])
                break

        self.assertTrue(found)

    def test_insert_games(self):
        games = OrderedDict({
            "2015102500": {
                "away": "BUF",
                "day": 25,
                "eid": "2015102500",
                "gamekey": "56595",
                "home": "JAC",
                "meridiem": "AM",
                "month": 10,
                "season_type": "REG",
                "time": "9:30",
                "wday": "Sun",
                "week": 7,
                "year": 2015
            },
            "2015102501": {
                "away": "CLE",
                "day": 25,
                "eid": "2015102501",
                "gamekey": "56601",
                "home": "STL",
                "meridiem": "PM",
                "month": 10,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 7,
                "year": 2015
            },
            "2015102502": {
                "away": "HOU",
                "day": 25,
                "eid": "2015102502",
                "gamekey": "56599",
                "home": "MIA",
                "month": 10,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 7,
                "year": 2015
            }
        })

        teams = [['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'],
                 ['JAC', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars'],
                 ['CLE', 'Cleveland', 'Browns', 'Cleveland Browns'],
                 ['STL', 'St. Louis', 'Rams', 'St. Louis Rams'],
                 ['HOU', 'Houston', 'Texans', 'Houstaon Texans'],
                 ['MIA', 'Miami', 'Dolphins', 'Miami Dolphins']]
        self.db.insert_teams(teams)
        self.db.insert_games(games)

        columns = self.db.get_table_column_names('Games')

        res = self.db.cursor.execute("SELECT * FROM Games").fetchall()
        self.assertEqual(len(res), len(games))

        for row in res:
            self.assertIn(row[columns.index('eid')], games)
            for idx, col in enumerate(columns):
                if col not in games[row[columns.index('eid')]]:
                    self.assertEqual(col, 'meridiem')
                    self.assertIsNone((row[idx]))
                    continue

                self.assertEqual(row[idx],
                                 games[row[columns.index('eid')]][col])

    def test_insert_more_than_999_games(self):
        info = {
            "away": "BUF",
            "day": 25,
            "gamekey": "56595",
            "home": "JAC",
            "meridiem": "AM",
            "month": 10,
            "season_type": "REG",
            "time": "9:30",
            "wday": "Sun",
            "week": 7,
            "year": 2015
        }
        games = OrderedDict({})
        for i in range(999*3 + 500):
            games[str(i).zfill(10)] = {**info, **{'eid': str(i).zfill(10)}}

        self.db.insert_teams(nflgame.teams)
        self.db.insert_games(games)

        columns = self.db.get_table_column_names('Games')
        games_inserted = \
            self.db.cursor.execute("SELECT * FROM Games").fetchall()
        self.assertEqual(len(games_inserted), len(games))

        for row in games_inserted:
            self.assertIn(row[columns.index('eid')], games)
            for idx, col in enumerate(columns):
                if col not in games[row[columns.index('eid')]]:
                    self.assertEqual(col, 'meridiem')
                    self.assertIsNone((row[idx]))
                    continue

                self.assertEqual(row[idx],
                                 games[row[columns.index('eid')]][col])

    def test_insert_player_game_statistics_invalid_column(self):
        self.assertRaises(RuntimeError, self.db.insert_player_game_statistics,
                          '1234567890', '2015102500', {'failure': 42})

    def test_insert_player_game_statistics_all_columns(self):
        player_data = {
            "birthdate": "2/9/1997",
            "college": "Penn State",
            "first_name": "Saquon",
            "full_name": "Saquon Barkley",
            "gsis_id": "00-0034844",
            "height": 72,
            "last_name": "Barkley",
            "number": 26,
            "position": "RB",
            "profile_id": 2559901,
            "profile_url": "http://www.nfl.com/player/saquonbarkley/2559901/profile",
            "status": "ACT",
            "team": "NYG",
            "weight": 233,
            "years_pro": 2
        }
        player = Player(player_data)
        self.db.insert_players(player)

        self.db.insert_teams([
            ['NYG', 'New York G', 'Giants', 'New York Giants'],
            ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']
        ])

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Player_Game_Statistics'))
        stats = {}
        for i, c in enumerate(valid_columns):
            if c == 'player_id' or c == 'eid':
                continue
            stats[c] = i + 1

        self.db.insert_player_game_statistics(player.player_id,
                                              next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Player_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'player_id':
                self.assertEqual(res[i], player.player_id)
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats[c])

    def test_insert_player_game_statistics_some_columns(self):
        player_data = {
            "birthdate": "2/9/1997",
            "college": "Penn State",
            "first_name": "Saquon",
            "full_name": "Saquon Barkley",
            "gsis_id": "00-0034844",
            "height": 72,
            "last_name": "Barkley",
            "number": 26,
            "position": "RB",
            "profile_id": 2559901,
            "profile_url": "http://www.nfl.com/player/saquonbarkley/2559901/profile",
            "status": "ACT",
            "team": "NYG",
            "weight": 233,
            "years_pro": 2
        }
        player = Player(player_data)
        self.db.insert_players(player)

        self.db.insert_teams([
            ['NYG', 'New York G', 'Giants', 'New York Giants'],
            ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']
        ])

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        stats = {
            'passing_tds': 4,
            'receiving_rec': 3,
            'rushing_tds': 2,
            'rushing_yds': 112,
            'receiving_yds': 23
        }

        self.db.insert_player_game_statistics(player.player_id,
                                              next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Player_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        valid_columns = self.db.get_table_column_names('Player_Game_Statistics')
        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'player_id':
                self.assertEqual(res[i], player.player_id)
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats.get(c, 0))

    def test_insert_player_game_statistics_no_columns(self):
        player_data = {
            "birthdate": "2/9/1997",
            "college": "Penn State",
            "first_name": "Saquon",
            "full_name": "Saquon Barkley",
            "gsis_id": "00-0034844",
            "height": 72,
            "last_name": "Barkley",
            "number": 26,
            "position": "RB",
            "profile_id": 2559901,
            "profile_url": "http://www.nfl.com/player/saquonbarkley/2559901/profile",
            "status": "ACT",
            "team": "NYG",
            "weight": 233,
            "years_pro": 2
        }
        player = Player(player_data)
        self.db.insert_players(player)

        self.db.insert_teams([
            ['NYG', 'New York G', 'Giants', 'New York Giants'],
            ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']
        ])

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        stats = {}

        self.db.insert_player_game_statistics(player.player_id,
                                              next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Player_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        valid_columns = self.db.get_table_column_names('Player_Game_Statistics')
        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'player_id':
                self.assertEqual(res[i], player.player_id)
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats.get(c, 0))

    def test_insert_team_game_statistics_invalid_column(self):
        self.assertRaises(RuntimeError, self.db.insert_team_game_statistics,
                          'NYG', '2015102500', {'failure': 42})

    def test_insert_team_game_statistics_all_columns(self):
        teams = [['NYG', 'New York G', 'Giants', 'New York Giants'],
                 ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']]
        self.db.insert_teams(teams)

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Team_Game_Statistics'))
        stats = {}
        for i, c in enumerate(valid_columns):
            if c == 'team' or c == 'eid':
                continue
            stats[c] = i + 1

        self.db.insert_team_game_statistics(teams[0][0],
                                            next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Team_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'team':
                self.assertEqual(res[i], teams[0][0])
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats[c])

    def test_insert_team_game_statistics_some_columns(self):
        teams = [['NYG', 'New York G', 'Giants', 'New York Giants'],
                 ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']]
        self.db.insert_teams(teams)

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Team_Game_Statistics'))
        stats = {
            'passing_tds': 4,
            'receiving_rec': 3,
            'rushing_tds': 2,
            'rushing_yds': 112,
            'receiving_yds': 23
        }

        self.db.insert_team_game_statistics(teams[0][0],
                                            next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Team_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'team':
                self.assertEqual(res[i], teams[0][0])
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats.get(c, 0))

    def test_insert_team_game_statistics_no_columns(self):
        teams = [['NYG', 'New York G', 'Giants', 'New York Giants'],
                 ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']]
        self.db.insert_teams(teams)

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "NYG",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Team_Game_Statistics'))
        stats = {}

        self.db.insert_team_game_statistics(teams[0][0],
                                            next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Team_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'team':
                self.assertEqual(res[i], teams[0][0])
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats.get(c, 0))

    def test_insert_team_game_statistics_alternate_abbreviation(self):
        teams = [['JAC', 'New York G', 'Giants', 'New York Giants', 'JAX'],
                 ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']]
        self.db.insert_teams(teams)

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "JAX",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Team_Game_Statistics'))
        stats = {}
        for i, c in enumerate(valid_columns):
            if c == 'team' or c == 'eid':
                continue
            stats[c] = i + 1

        self.db.insert_team_game_statistics(teams[0][-1],
                                            next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Team_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'team':
                self.assertEqual(res[i], teams[0][0])
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], stats[c])

    def test_insert_team_game_statistics_alternate_abbreviation_no_data(self):
        teams = [['JAC', 'New York G', 'Giants', 'New York Giants', 'JAX'],
                 ['PHI', 'Philadelphia', 'Eagles', 'Philadelphia Eagles']]
        self.db.insert_teams(teams)

        game = {
            "2019122911": {
                "away": "PHI",
                "day": 29,
                "eid": "2019122911",
                "gamekey": "58151",
                "home": "JAX",
                "meridiem": "PM",
                "month": 12,
                "season_type": "REG",
                "time": "1:00",
                "wday": "Sun",
                "week": 17,
                "year": 2019
            }
        }
        self.db.insert_games(game)

        valid_columns \
            = list(self.db.get_table_column_names('Team_Game_Statistics'))

        stats = {}
        self.db.insert_team_game_statistics(teams[0][-1],
                                            next(iter(game.keys())), stats)

        res = self.db.cursor.execute("SELECT * FROM "
                                     "Team_Game_Statistics").fetchall()
        self.assertEqual(len(res), 1)
        res = res[0]

        self.assertEqual(len(res), len(valid_columns))
        for i, c in enumerate(valid_columns):
            if c == 'team':
                self.assertEqual(res[i], teams[0][0])
                continue
            elif c == 'eid':
                self.assertEqual(res[i], next(iter(game.keys())))
                continue
            self.assertEqual(res[i], 0)
