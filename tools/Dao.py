import pymysql

from auth import authDB


class Dao:
    def __init__(self):
        self.user = authDB.USER
        self.pwd = authDB.PASSWORD
        self.host = authDB.HOST
        self.db = authDB.DB

    def __query_db(self):
        db = pymysql.connect(host=self.host,
                             user=self.user,
                             passwd=self.pwd,
                             db=self.db,
                             charset='utf32',
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
        res = False
        try:
            cursor.execute('SELECT * FROM table')
            res = [t[0] for t in cursor.fetchall()]
        finally:
            cursor.close()
        return res
