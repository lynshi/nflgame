import sqlite3 as sql
import nflgame


class NFLDatabase:
    def __init__(self, db_file_name):
        """
        Build a SQLite3 database in the file found at db_file_name.

        :param db_file_name: name of file to store database in
        """
        self.db_file_name = db_file_name
        self.conn = sql.connect(self.db_file_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute('PRAGMA foreign_keys = ON')

    def get_table_column_names(self, table_name):
        """
        Return table columns, in order, for table table_name

        :param table_name: name of table to fetch column names for
        :return: list of column names in order of schema

        :raises RuntimeError: if table_name is not a valid table name
        """
        valid_tables = {'Players', 'Games', 'Teams', 'Player_Game_Statistics',
                        'Team_Game_Statistics'}
        if table_name not in valid_tables:
            raise RuntimeError(table_name + ' is not a valid table')

        res = self.cursor.execute('PRAGMA table_info(' + table_name +
                                  ')').fetchall()
        return [col[1] for col in res]

    def _drop_table(self, table_name):
        """
        Drop table table_name

        :param table_name: name of table to drop
        :return: None

        :raises RuntimeError: if table_name is not a valid table name
        """
        valid_tables = {'Players', 'Games', 'Teams', 'Player_Game_Statistics',
                        'Team_Game_Statistics'}
        if table_name not in valid_tables:
            raise RuntimeError(table_name + ' is not a valid table')

        self.cursor.execute("DROP TABLE " + table_name)
        self.commit()

    def reset(self):
        """
        Drop all tables in database.

        :return: None
        """

        # Ordered this way to prevent key errors on drop
        valid_tables = ['Player_Game_Statistics', 'Team_Game_Statistics',
                        'Games', 'Teams', 'Players', ]
        for table in valid_tables:
            self._drop_table(table)

    def close(self):
        """
        Call to explicitly close db connection

        :return: None
        """
        self.conn.close()

    def __del__(self):
        """
        Close Connection instance

        :return: None
        """
        self.close()

    def commit(self):
        """
        Commit database

        :return: None
        """
        self.conn.commit()

    def create_players_table(self):
        """
        Create Players table to store player information

        :return: None
        """
        self.cursor.execute("""
            CREATE TABLE Players (
                player_id CHAR(10) PRIMARY KEY NOT NULL, 
                gsis_name VARCHAR(50) NOT NULL, 
                full_name VARCHAR(50) NOT NULL, 
                first_name VARCHAR(50) NOT NULL, 
                last_name VARCHAR(50) NOT NULL,
                team VARCHAR(3) NOT NULL, 
                position VARCHAR(6) NOT NULL, 
                profile_id INT NOT NULL, 
                profile_url VARCHAR(100) NOT NULL, 
                uniform_number INT NOT NULL,
                birthdate VARCHAR(10) NOT NULL, 
                college VARCHAR(50) NOT NULL, 
                height REAL NOT NULL, 
                weight REAL NOT NULL, 
                years_pro INT NOT NULL, 
                status VARCHAR(10) NOT NULL
            )
        """, )
        self.commit()

    def create_teams_table(self):
        """
        Create Teams table to store team information

        :return: None
        """
        self.cursor.execute("""
            CREATE TABLE Teams (
                team VARCHAR(3) PRIMARY KEY NOT NULL,
                city VARCHAR(50) NOT NULL,
                team_name VARCHAR(50) NOT NULL,
                full_name VARCHAR(50) NOT NULL,
                alt_abbrev VARCHAR(3)
            )
        """)

        self.commit()

    def create_games_table(self):
        """
        Create Games table to store game schedule information.

        :return: None
        """

        self.cursor.execute("""
            CREATE TABLE Games (
                away VARCHAR(3) NOT NULL,
                day INT NOT NULL,
                eid VARCHAR(10) PRIMARY KEY NOT NULL,
                gamekey VARCHAR(10) NOT NULL,
                home VARCHAR(3) NOT NULL,
                season_type VARCHAR(4) NOT NULL,
                time VARCHAR(5) NOT NULL,
                meridiem CHAR(2),
                wday CHAR(3) NOT NULL,
                week INT NOT NULL,
                year INT NOT NULL,
                FOREIGN KEY (away) REFERENCES Teams(team),
                FOREIGN KEY (home) REFERENCES Teams(team)
            )
        """)

        self.commit()

    def create_player_game_statistics_table(self):
        """
        Create Player_Game_Statistics table to store accumulated statistics for
        each player in each game.

        :return: None
        """

        self.cursor.execute("""
            CREATE TABLE Player_Game_Statistics (
                player_id CHAR(10) NOT NULL,
                eid VARCHAR(10) NOT NULL,
                defense_ast REAL DEFAULT 0,
                defense_ffum REAL DEFAULT 0,
                defense_int REAL DEFAULT 0,
                defense_sk REAL DEFAULT 0,
                defense_tkl REAL DEFAULT 0,
                fumbles_lost REAL DEFAULT 0,
                fumbles_rcv REAL DEFAULT 0,
                fumbles_tot REAL DEFAULT 0,
                fumbles_trcv REAL DEFAULT 0,
                fumbles_yds REAL DEFAULT 0,
                kicking_fga REAL DEFAULT 0,
                kicking_fgm REAL DEFAULT 0,
                kicking_fgyds REAL DEFAULT 0,
                kicking_totpfg REAL DEFAULT 0,
                kicking_xpa REAL DEFAULT 0,
                kicking_xpb REAL DEFAULT 0,
                kicking_xpmade REAL DEFAULT 0,
                kicking_xpmissed REAL DEFAULT 0,
                kicking_xptot REAL DEFAULT 0,
                kickret_avg REAL DEFAULT 0,
                kickret_lng REAL DEFAULT 0,
                kickret_lngtd REAL DEFAULT 0,
                kickret_ret REAL DEFAULT 0,
                kickret_tds REAL DEFAULT 0,
                passing_att REAL DEFAULT 0,
                passing_cmp REAL DEFAULT 0,
                passing_ints REAL DEFAULT 0,
                passing_tds REAL DEFAULT 0,
                passing_twopta REAL DEFAULT 0,
                passing_twoptm REAL DEFAULT 0,
                passing_yds REAL DEFAULT 0,
                punting_avg REAL DEFAULT 0,
                punting_i20 REAL DEFAULT 0,
                punting_lng REAL DEFAULT 0,
                punting_pts REAL DEFAULT 0,
                punting_yds REAL DEFAULT 0,
                puntret_avg REAL DEFAULT 0,
                puntret_lng REAL DEFAULT 0,
                puntret_lngtd REAL DEFAULT 0,
                puntret_ret REAL DEFAULT 0,
                puntret_tds REAL DEFAULT 0,
                receiving_lng REAL DEFAULT 0,
                receiving_lngtd REAL DEFAULT 0,
                receiving_rec REAL DEFAULT 0,
                receiving_tds REAL DEFAULT 0,
                receiving_twopta REAL DEFAULT 0,
                receiving_twoptm REAL DEFAULT 0,
                receiving_yds REAL DEFAULT 0,
                rushing_att REAL DEFAULT 0,
                rushing_lng REAL DEFAULT 0,
                rushing_lngtd REAL DEFAULT 0,
                rushing_tds REAL DEFAULT 0,
                rushing_twopta REAL DEFAULT 0,
                rushing_twoptm REAL DEFAULT 0,
                rushing_yds REAL DEFAULT 0,
                PRIMARY KEY (player_id, eid),
                FOREIGN KEY (eid) REFERENCES Games,
                FOREIGN KEY (player_id) REFERENCES Players 
            )
        """)
        self.commit()

    def create_team_game_statistics_table(self):
        """
        Create Team_Game_Statistics table to store accumulated statistics for
        each team in each game. Columns are created for each element in
        stat_columns.

        :param stat_columns: list of statistic names to create columns for
        :return: None
        """

        self.cursor.execute("""
            CREATE TABLE Team_Game_Statistics (
                team VARCHAR(3) NOT NULL,
                eid VARCHAR(10) NOT NULL,
                defense_ast REAL DEFAULT 0,
                defense_ffum REAL DEFAULT 0,
                defense_int REAL DEFAULT 0,
                defense_sk REAL DEFAULT 0,
                defense_tkl REAL DEFAULT 0,
                fumbles_lost REAL DEFAULT 0,
                fumbles_rcv REAL DEFAULT 0,
                fumbles_tot REAL DEFAULT 0,
                fumbles_trcv REAL DEFAULT 0,
                fumbles_yds REAL DEFAULT 0,
                kicking_fga REAL DEFAULT 0,
                kicking_fgm REAL DEFAULT 0,
                kicking_fgyds REAL DEFAULT 0,
                kicking_totpfg REAL DEFAULT 0,
                kicking_xpa REAL DEFAULT 0,
                kicking_xpb REAL DEFAULT 0,
                kicking_xpmade REAL DEFAULT 0,
                kicking_xpmissed REAL DEFAULT 0,
                kicking_xptot REAL DEFAULT 0,
                kickret_avg REAL DEFAULT 0,
                kickret_lng REAL DEFAULT 0,
                kickret_lngtd REAL DEFAULT 0,
                kickret_ret REAL DEFAULT 0,
                kickret_tds REAL DEFAULT 0,
                passing_att REAL DEFAULT 0,
                passing_cmp REAL DEFAULT 0,
                passing_ints REAL DEFAULT 0,
                passing_tds REAL DEFAULT 0,
                passing_twopta REAL DEFAULT 0,
                passing_twoptm REAL DEFAULT 0,
                passing_yds REAL DEFAULT 0,
                punting_avg REAL DEFAULT 0,
                punting_i20 REAL DEFAULT 0,
                punting_lng REAL DEFAULT 0,
                punting_pts REAL DEFAULT 0,
                punting_yds REAL DEFAULT 0,
                puntret_avg REAL DEFAULT 0,
                puntret_lng REAL DEFAULT 0,
                puntret_lngtd REAL DEFAULT 0,
                puntret_ret REAL DEFAULT 0,
                puntret_tds REAL DEFAULT 0,
                receiving_lng REAL DEFAULT 0,
                receiving_lngtd REAL DEFAULT 0,
                receiving_rec REAL DEFAULT 0,
                receiving_tds REAL DEFAULT 0,
                receiving_twopta REAL DEFAULT 0,
                receiving_twoptm REAL DEFAULT 0,
                receiving_yds REAL DEFAULT 0,
                rushing_att REAL DEFAULT 0,
                rushing_lng REAL DEFAULT 0,
                rushing_lngtd REAL DEFAULT 0,
                rushing_tds REAL DEFAULT 0,
                rushing_twopta REAL DEFAULT 0,
                rushing_twoptm REAL DEFAULT 0,
                rushing_yds REAL DEFAULT 0,
                PRIMARY KEY (team, eid),
                FOREIGN KEY (eid) REFERENCES Games,
                FOREIGN KEY (team) REFERENCES Teams
            )
        """)
        self.commit()

    def insert_players(self, players):
        """
        Insert player data from players into the Players table. If players is a
        single item, it is converted to a list automatically.

        :param players: list of Player objects whose data to insert
        :return: None
        """

        if isinstance(players, nflgame.player.Player) is True:
            players = [players]

        if isinstance(players, list) is False \
                and isinstance(players, tuple) is False:
            players = list(players)

        def reset_defaults():
            """
            Reset default values for query and params after cursor execution.

            :return: None
            """
            nonlocal query, params
            query = """INSERT INTO Players Values """
            params = []

        def execute_insert():
            """
            Insert game data.

            :return: None
            """
            nonlocal params, query
            params = tuple(params)
            query += row_placeholder * (len(params) // len(attributes))
            self.cursor.execute(query[:-2], params)

        query = ''
        params = []
        attributes = [
            'player_id', 'gsis_name', 'full_name', 'first_name',
            'last_name', 'team', 'position', 'profile_id', 'profile_url',
            'uniform_number', 'birthdate', 'college', 'height', 'weight',
            'years_pro', 'status'
        ]
        row_placeholder = '(' + '?,' * (len(attributes) - 1) + '?), '

        reset_defaults()
        max_games = 999  # From SQLite
        for p in players:
            for a in attributes:
                params.append(getattr(p, a))

            if len(params) + len(attributes) > max_games:
                execute_insert()
                reset_defaults()

        if len(params) > 0:
            execute_insert()

        self.commit()

    def insert_teams(self, teams):
        """
        Insert team data from teams into the Teams table. If teams is a
        single item, it is converted to a list automatically.

        :param teams: list of team data of the format
            [[Abbreviation, City, Team Name, Full Name
                (, Alternatve Abbreviation)],...]
            e.g. [[NYG, New York G, Giants, New York Giants],
                  [CLE, Cleveland, Browns, Cleveland Browns],
                  [LA, Los Angeles, Rams, Los Angeles Rams, LAR]]
            Additional items in the list are allowed but only the first 5 are
            used.
        :return: None
        """

        if isinstance(teams[0], list) is False \
                and isinstance(teams[0], tuple) is False:
            teams = [teams]

        query = """INSERT INTO Teams Values """
        params = []
        for t in teams:
            if len(t) > 4:
                query += '(?,?,?,?,?), '
                params += t[:5]
            else:
                query += '(?,?,?,?,?), '
                params += t + [t[0]]
        params = tuple(params)

        self.cursor.execute(query[:-2], params)
        self.commit()

    def insert_games(self, games):
        """
        Insert game schedule data from games into the Games table.

        :param games: OrderedDict of schedule data as in nflgames.sched.games
        :return: None
        """

        def reset_defaults():
            """
            Reset default values for query and params after cursor execution.

            :return: None
            """
            nonlocal query, params
            query = """INSERT INTO Games Values """
            params = []

        def execute_insert():
            """
            Insert game data.

            :return: None
            """
            nonlocal params, query
            params = tuple(params)
            query += row_placeholder * (len(params) // len(attributes))
            self.cursor.execute(query[:-2], params)

        attributes = ['away', 'day', 'eid', 'gamekey', 'home', 'season_type',
                      'time', 'meridiem', 'wday', 'week', 'year']

        # SQLITE_MAX_VARIABLE_NUMBER is inaccessible and cannot be changed.
        # The default value is 999, so that is what shall be used here.
        max_games = 999
        query = ''
        params = []
        row_placeholder = '(' + '?,' * (len(attributes) - 1) + '?), '
        reset_defaults()
        for eid, info in games.items():
            for attr in attributes:
                if attr == 'meridiem':
                    params.append(info.get('meridiem', None))
                    continue
                # Some abbreviations are not reliable
                if attr in {'home', 'away'}:
                    go_to_next = False
                    for team in nflgame.teams:
                        if info[attr] == team[0]:
                            break
                        elif info[attr] == team[-1]:
                            params.append(team[0])
                            go_to_next = True
                            break
                    if go_to_next is True:
                        continue

                params.append(info[attr])

            if len(params) + len(attributes) > max_games:
                execute_insert()
                reset_defaults()

        if len(params) > 0:
            execute_insert()

        self.commit()

    def insert_player_game_statistics(self, player_id, eid, player_stats):
        """
        Insert a player's statistics for a single game.

        :param player_id: player_id for player whose statistics
            are to be inserted
        :param eid: id for game in which the statistics were accumulated
        :param player_stats: dict of player statistics, as would be
            obtained with player._stats
        :return: None

        :raises RuntimeError: if any key in team_stats does not correspond to
            a valid statistic name
        """

        valid_columns = {
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

        columns = '(player_id, eid, '
        for stat in player_stats.keys():
            if stat not in valid_columns:
                raise RuntimeError(stat + ' is not a valid column in '
                                          'Player_Game_Statistics')
            columns += stat + ', '

        query = 'INSERT INTO Player_Game_Statistics ' \
                + columns[:-2] + ') Values (?,?,' \
                + '?,' * (len(player_stats) - 1) + '?)'

        self.cursor.execute(query,
                            (player_id, eid) + tuple(player_stats.values()))
        self.commit()

    def insert_team_game_statistics(self, team, eid, team_stats):
        """
        Insert a team's statistics for a single game.

        :param team: team whose statistics are to be inserted. Follow the
            abbreviation in nflgame.teams
        :param eid: id for game in which the statistics were accumulated
        :param team_stats: dict of team statistics, formatted as though
            obtained from player._stats
        :return: None

        :raises RuntimeError: if any key in team_stats does not correspond to
            a valid statistic name
        """

        valid_columns = {
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

        columns = '(team, eid, '
        for stat in team_stats.keys():
            if stat not in valid_columns:
                raise RuntimeError(stat + ' is not a valid column in '
                                          'Team_Game_Statistics')
            columns += stat + ', '

        query = 'INSERT INTO Team_Game_Statistics ' \
                + columns[:-2] + ') Values (?,?,' \
                + '?,' * (len(team_stats) - 1) + '?)'

        self.cursor.execute(query,
                            (team, eid) + tuple(team_stats.values()))
        self.commit()
