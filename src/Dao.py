import pymysql
import toml


class Dao:
    def __init__(self):
        auth = toml.load("../auth/auth.toml")
        self.user = auth.get("user")
        self.pwd = auth.get("password")
        self.host = auth.get("host")
        self.db = auth.get("db")

    def __query_db(self):
        db = pymysql.connect(host=self.host,
                             user=self.user,
                             passwd=self.pwd,
                             db=self.db,
                             port=3306,
                             charset='utf8',
                             cursorclass=pymysql.cursors.SSCursor)
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
