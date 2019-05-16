import sqlite3 as sql


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
        if isinstance(players, list) is False \
                and isinstance(players, tuple) is False:
            players = [players]

        query = """INSERT INTO Players Values """
        attributes = [
            'player_id', 'gsis_name', 'full_name', 'first_name',
            'last_name', 'team', 'position', 'profile_id', 'profile_url',
            'uniform_number', 'birthdate', 'college', 'height', 'weight',
            'years_pro', 'status'
        ]

        params = []
        for p in players:
            for a in attributes:
                params.append(getattr(p, a))
        params = tuple(params)

        row_placeholder = '(' + '?,' * (len(attributes) - 1) + '?), '
        query += row_placeholder * len(players)

        self.cursor.execute(query[:-2], params)
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

        if (isinstance(teams, list) is False
            or isinstance(teams[0], list) is False) \
                and (isinstance(teams, tuple) is False
                     or isinstance(teams[0], tuple) is False):
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
        Insert game schedule data from games into the Games table. If games is a
        single item, it is converted to a list automatically.

        :param games: list of games following the format in
            nflgame/schedule.json
        :return: None
        """

        if (isinstance(games, list) is False
            or isinstance(games[0], list) is False) \
                and (isinstance(games, tuple) is False
                     or isinstance(games[0], tuple) is False):
            games = [games]

        query = """INSERT INTO Games Values """
        params = []
        attributes = ['away', 'day', 'eid', 'gamekey', 'home', 'season_type',
                      'time', 'meridiem', 'wday', 'week', 'year']
        for g in games:
            g = g[1]
            for attr in attributes:
                if attr == 'meridiem':
                    params.append(g.get('meridiem', None))
                    continue
                params.append(g[attr])
        params = tuple(params)

        row_placeholder = '(' + '?,' * (len(attributes) - 1) + '?), '
        query += row_placeholder * len(games)

        self.cursor.execute(query[:-2], params)
        self.commit()

    def insert_player_game_statistics(self, player_id, player_stats):
        """
        Insert a player's statistics for a single game.

        :param player_id: player_id for player whose statistics
            are to be inserted
        :param player_stats: OrderedDict of player statistics, as would be
            obtained with player._stats
        :return: None
        """

        columns = ('player_id',) + tuple(player_stats.keys())
        query = 'INSERT INTO Player_Game_Statistics (' \
                + '?,' * (len(columns) - 1) + '?) Values (' \
                + '?,' * (len(columns) - 1) + '?)'

        self.cursor.execute(query,
                            columns + (player_id,) + player_stats.values())
        self.commit()

    def insert_team_game_statistics(self, team, team_stats):
        """
        Insert a team's statistics for a single game.

        :param team: team whose statistics are to be inserted. Follow the
            abbreviation in nflgame.teams
        :param team_stats: OrderedDict of team statistics, formatted as though
            obtained from player._stats
        :return: None
        """

        columns = ('team',) + tuple(team_stats.keys())
        query = 'INSERT INTO Team_Game_Statistics (' \
                + '?,' * (len(columns) - 1) + '?) Values (' \
                + '?,' * (len(columns) - 1) + '?)'

        self.cursor.execute(query,
                            columns + (team,) + team_stats.values())
        self.commit()
