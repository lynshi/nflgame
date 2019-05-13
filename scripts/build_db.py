import database.database as nfldb
import nflgame


def build_db():
    season = 2009
    while True:
        print('finding ' + str(season))
        games = nflgame.games(season)
        print(len(games))
        season += 1


if __name__ == '__main__':
    build_db()