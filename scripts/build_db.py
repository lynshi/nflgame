import database.database as nfldb
import nflgame


def build_db():
    players = nflgame.players
    for p, player in players.items():
        if player.team == '':
            continue
        print(player.team)
        exit()


if __name__ == '__main__':
    build_db()
