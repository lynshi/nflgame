from nfldatabase.dbbuilder import NFLdbBuilder
import json
import nflgame


def build_db():
    stats = NFLdbBuilder._find_stat_columns()
    s = ''
    expected_values = {}
    for stat in sorted(stats):
        s += stat + ' REAL DEFAULT 0,\n'
        expected_values[stat] = ('REAL', 0, 0, 0)

    for stat in sorted(expected_values.keys()):
        print('\'' + stat + '\': (\'REAL\', 0, \'0\', 0),')
    print(s[:-2])


if __name__ == '__main__':
    build_db()
