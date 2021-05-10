#!/usr/bin/python3
from src.dao.dao_mongo_db import DaoMongoDB
from src.dao.dao_mysql_db import DaoMySQLDB


class Dao:
    def __init__(self, type_db: bool):
        """
        DAO init
        @param type_db: True = relationalDB, False = MongoDB
        """
        self.dao_type = DaoMySQLDB() if type_db else DaoMongoDB()

    def build_db(self):
        """
        Builds the DB
        """
        self.dao_type.build_db()


if __name__ == '__main__':
    """ DAO """
    d = Dao(True)
    d.build_db()
