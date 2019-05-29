import os
import unittest

from nfldatabase.dbbuilder import NFLdbBuilder, find_stat_columns
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
        found_columns = find_stat_columns()
        expected_columns = {
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

        self.db.run(update=True)
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
