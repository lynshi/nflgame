import sqlite3 as sql
import unittest

import database.nfl_database as nfldb


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = nfldb.NFLDatabase(':memory:')

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
