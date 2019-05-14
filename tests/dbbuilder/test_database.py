import sqlite3 as sql
import unittest

from nflgame.player import Player
import database.nfl_database as nfldb


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = nfldb.NFLDatabase(':memory:')
        self.db.create_players_table()

    def test_close(self):
        self.db.close()
        self.assertRaises(sql.ProgrammingError, self.db.cursor.execute,
                          'CREATE TABLE Failure')

    def test_player_table_creation(self):
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

    def test_insert_player(self):
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
        players = []
        for pid in test_set:
            players.append(Player(test_set[pid]))

        self.db.insert_players(players)
        inserted = self.db.cursor.execute("SELECT * FROM Players")
        res = self.db.cursor.execute('PRAGMA table_info(Players)').fetchall()

        columns = [col[1] for col in res]

        for res in inserted:
            self.assertIn(res[0], test_set)
            to_match = test_set[res[0]]
            for idx, name in enumerate(columns):
                if name not in to_match:
                    self.assertTrue(res[idx] == '' or res[idx] == 0)
                    continue

                self.assertEqual(res[idx], to_match[name])
