import unittest

from nfldatabase.dbbuilder import NFLdbBuilder


class TestNFLdbBuilder(unittest.TestCase):
    def setUp(self):
        self.db = NFLdbBuilder(':memory:')

    def test_table_creation(self):
        res = self.db.db.cursor.execute("SELECT name FROM sqlite_master "
                                        "WHERE type='table'")
        table_names = set()
        for r in res:
            table_names.add(r[0])
        expected_table_names = {'Players', 'Games', 'Teams',
                                'Player_Game_Statistics',
                                'Team_Game_Statistics'}
        self.assertSetEqual(table_names, expected_table_names)

    def test_find_stat_columns(self):
        found_columns = self.db._find_stat_columns()
        expected_columns = {
            'defense_ast', 'defense_ffum', 'defense_int', 'defense_sk',
            'defense_tkl', 'fumbles_lost', 'fumbles_rcv', 'fumbles_tot',
            'fumbles_trcv', 'fumbles_yds', 'kicking_fga', 'kicking_fgm',
            'kicking_fgyds', 'kicking_totpfg', 'kicking_xpa', 'kicking_xpb',
            'kicking_xpmade', 'kicking_xpmissed', 'kicking_xptot',
            'kickret_avg', 'kickret_lng', 'kickret_lngtd', 'kickret_ret',
            'kickret_tds', 'passing_att', 'passing_cmp', 'passing_ints',
            'passing_tds', 'passing_twopta', 'passing_twoptm', 'passing_yds',
            'punting_avg', 'punting_i20', 'punting_lng', 'punting_pts',
            'punting_yds', 'puntret_avg', 'puntret_lng', 'puntret_lngtd',
            'puntret_ret', 'puntret_tds', 'receiving_lng', 'receiving_lngtd',
            'receiving_rec', 'receiving_tds', 'receiving_twopta',
            'receiving_twoptm', 'receiving_yds', 'rushing_att', 'rushing_lng',
            'rushing_lngtd', 'rushing_tds', 'rushing_twopta', 'rushing_twoptm',
            'rushing_yds'
        }

        self.assertSetEqual(found_columns, expected_columns)
