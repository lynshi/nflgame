import sqlite3 as sql
import unittest

import database.database as nfldb


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = nfldb.NFLDatabase(':memory:')

    def test_close(self):
        self.db.close()
        self.assertRaises(sql.ProgrammingError, self.db.cursor.execute,
                          'CREATE TABLE Failure')
