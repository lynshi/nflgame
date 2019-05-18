from nfldatabase.dbbuilder import NFLdbBuilder

import os
import sys


def build_db():
    file_directory = os.path.dirname(os.path.abspath(__file__))
    separator = '\\' if sys.platform in {'win32', 'cygwin'} else '/'
    prefix = file_directory[:file_directory.find('nflgame' + separator +
                                                 'scripts')]
    db_path = os.path.join(prefix, 'nflgame', 'db')

    if os.path.isdir(db_path) is False:
        os.makedirs(db_path)

    db = NFLdbBuilder(os.path.join(db_path, 'nfl.db'))
    db.run()


if __name__ == '__main__':
    build_db()
