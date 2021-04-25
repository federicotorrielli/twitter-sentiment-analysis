#!/usr/bin/python3
from pymysql import cursors, connect
from toml import load


class Dao:
    def __init__(self):
        auth = load("../auth/auth.toml")
        self.user = auth.get("user")
        self.pwd = auth.get("password")
        self.host = auth.get("host")
        self.db = auth.get("db")

    def __query_db(self):
        db = connect(host=self.host,
                     port=3306,
                     user=self.user,
                     passwd=self.pwd,
                     db=self.db,
                     charset='utf8',
                     cursorclass=cursors.SSCursor)
        cur = db.cursor()
        cur.execute('SET NAMES utf8mb4')
        cur.execute("SET CHARACTER SET utf8mb4")
        cur.execute("SET character_set_connection=utf8mb4")
        db.commit()
        return cur

    def query_example(self) -> [str]:
        """ example """
        cursor = self.__query_db()
        res = []
        try:
            cursor.execute('SELECT * FROM sentiment')
            res = [t[0] for t in cursor.fetchall()]
        finally:
            cursor.close()
        return res


if __name__ == '__main__':
    """ DAO tests. """
    print(Dao().query_example())
