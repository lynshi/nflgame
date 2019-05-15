[![Build Status](https://travis-ci.org/lynshi/nflgame.svg?branch=master)](https://travis-ci.org/lynshi/nflgame) [![Coverage Status](https://coveralls.io/repos/github/lynshi/nflgame/badge.svg?branch=master)](https://coveralls.io/github/lynshi/nflgame?branch=master)

This repository is a fork of [MLDERES/nflgame](https://github.com/MLDERES/nflgame), which migrated [derek-adair/nflgame](https://github.com/derek-adair/nflgame) to Python 3. The Python 2 version is on PyPI as [nflgame-redux](https://pypi.org/project/nflgame).

I wanted to move everything into a database so accessing and manipulating the data is easier. The original author has a [project](https://github.com/BurntSushi/nfldb) for migration into a database, but unfortunately it is unmaintained. Since the Python 3 package has not been released yet, this fork is to help me build the database using the Python 3 implementation for forwards compatibility.
