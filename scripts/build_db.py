from nfldatabase.dbbuilder import NFLdbBuilder
import json
import nflgame


def build_db():
    db = NFLdbBuilder(':memory:')
    db.run()


if __name__ == '__main__':
    build_db()
