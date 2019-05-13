import database.database as nfldb
import nflgame


def build_db():
    season = 200
    while True:
        print('finding ' + str(season))
        games = nflgame.games(season)
        print('found ' + str(season))
        season += 1


if __name__ == '__main__':
    build_db()