import sqlite3 as sql
import unittest

import nflgame
from nflgame.player import Player
import nfldatabase.database as nfldb


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = nfldb.NFLDatabase(':memory:')
        self.db.create_players_table()
        self.db.create_teams_table()
        self.db.create_games_table()

    def test_close(self):
        self.db.close()
        self.assertRaises(sql.ProgrammingError, self.db.cursor.execute,
                          'CREATE TABLE Failure')

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
            'height': ('INT', 1, None, 0),
            'weight': ('INT', 1, None, 0),
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

        res = self.db.cursor.execute('PRAGMA table_info(Players)').fetchall()
        columns = [col[1] for col in res]

        self.db.insert_players(list(players.values()))
        inserted = self.db.cursor.execute("SELECT * FROM Players")

        for res in inserted:
            self.assertIn(res[0], test_set)
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

        res = self.db.cursor.execute('PRAGMA table_info(Players)').fetchall()
        columns = [col[1] for col in res]

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
        games = [
            [
                "2015102500",
                {
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
            ],
            [
                "2015102501",
                {
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
                }
            ],
            [
                "2015102502",
                {
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
            ]
        ]

        teams = [['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'],
                 ['JAC', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars'],
                 ['CLE', 'Cleveland', 'Browns', 'Cleveland Browns'],
                 ['STL', 'St. Louis', 'Rams', 'St. Louis Rams'],
                 ['HOU', 'Houston', 'Texans', 'Houstaon Texans'],
                 ['MIA', 'Miami', 'Dolphins', 'Miami Dolphins']]
        self.db.insert_teams(teams)
        self.db.insert_games(games)

        res = self.db.cursor.execute('PRAGMA table_info(Games)').fetchall()
        columns = [col[1] for col in res]

        res = self.db.cursor.execute("SELECT * FROM Games").fetchall()
        self.assertEqual(len(res), len(games))

        for row in res:
            found = False
            for game in games:
                if game[0] == row[columns.index('eid')]:
                    for idx, col in enumerate(columns):
                        if col not in game[1]:
                            self.assertEqual(col, 'meridiem')
                            self.assertIsNone((row[idx]))
                            continue

                        self.assertEqual(row[idx], game[1][col])
                    found = True
                    break

            self.assertTrue(found)

    def test_insert_one_team(self):
        game = [
            "2015102500",
            {
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
        ]

        teams = [['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'],
                 ['JAC', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars']]
        self.db.insert_teams(teams)
        self.db.insert_games(game)

        res = self.db.cursor.execute('PRAGMA table_info(Games)').fetchall()
        columns = [col[1] for col in res]

        res = self.db.cursor.execute("SELECT * FROM Games").fetchall()
        self.assertEqual(len(res), 1)

        row = res[0]
        for idx, col in enumerate(columns):
            if col not in game[1]:
                self.assertEqual(col, 'meridiem')
                self.assertIsNone((row[idx]))
                continue

            self.assertEqual(row[idx], game[1][col])

    def test_home_foreign_key_constraint_in_games(self):
        game = [
            "2015102500",
            {
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
        ]

        self.db.insert_teams(['BUF', 'Buffalo', 'Bills', 'Buffalo Bills'])
        self.assertRaises(sql.IntegrityError, self.db.insert_games, game)

    def test_away_foreign_key_constraint_in_games(self):
        game = [
            "2015102500",
            {
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
        ]

        self.db.insert_teams(
            ['JAC', 'Jacksonville', 'Jaguars', 'Jacksonville Jaguars'])
        self.assertRaises(sql.IntegrityError, self.db.insert_games, game)
