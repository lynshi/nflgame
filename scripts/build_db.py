from nfldatabase.dbbuilder import NFLdbBuilder
import json
import nflgame


def build_db():
    stats = NFLdbBuilder._find_stat_columns()
    s = ''
    for stat in sorted(stats):
        s += '\'' + stat + '\', '
    print(s[:-2])


if __name__ == '__main__':
    build_db()
