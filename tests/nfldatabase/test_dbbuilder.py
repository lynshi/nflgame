import os
import unittest

from nfldatabase.dbbuilder import NFLdbBuilder
import nflgame


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

    def test_open_existing_database(self):
        if os.path.isfile('test.db'):
            os.remove('test.db')

        db = NFLdbBuilder('test.db')
        try:
            db.db.insert_teams(nflgame.teams)

            db = NFLdbBuilder('test.db')

            # make sure data was not erased
            teams = db.db.cursor.execute("SELECT * FROM Teams").fetchall()
            self.assertEqual(len(teams), len(nflgame.teams))

        finally:
            db.db.close()
            os.remove('test.db')

    def test_run_update(self):
        counter = 0

        self.db._insert_teams()
        self.db._insert_games()
        self.db._insert_players()
        self.db._is_new_db = False
        for eid, info in nflgame.sched.games.items():
            if info['week'] == 0:
                continue

            # magic numbers at start because the end of the schedule can be
            # unplayed games
            if counter == 0:
                self.db.db.insert_team_game_statistics(info['home'], eid,
                                                       {})
            elif counter == 2:
                self.db.db.insert_team_game_statistics(info['away'], eid,
                                                       {})
            elif counter == 42:
                pass
            else:
                self.db.db.insert_team_game_statistics(info['home'], eid,
                                                       {})
                self.db.db.insert_team_game_statistics(info['away'], eid,
                                                       {})

            counter += 1

        self.db.run(reset=False, update=True)
        for eid, info in nflgame.sched.games.items():
            if info['year'] >= 2019 or info['week'] == 0:
                continue

            res = self.db.db.cursor.execute("SELECT team FROM "
                                            "Team_Game_Statistics "
                                            "WHERE eid = ?", (eid,)).fetchall()
            self.assertEqual(len(res), 2)
            teams = set()
            for team in [info['away'], info['home']]:
                db_teams = self.db.db.cursor.execute("SELECT Team FROM Teams "
                                                     "WHERE alt_abbrev = ?",
                                                     (team,)).fetchall()
                if len(db_teams) > 0:
                    team = db_teams[0][-1]
                teams.add(team)

            for t in res:
                self.assertIn(t[0], teams)
                teams.remove(t[0])

            self.assertEqual(len(teams), 0)
