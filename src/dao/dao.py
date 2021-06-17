#!/usr/bin/python3
from src.dao.dao_mongo_db import DaoMongoDB
from src.dao.dao_mysql_db import DaoMySQLDB
from src.datasets_manager import get_sentiment_words, get_sentiment_emoticons, get_sentiment_emojis


class Dao:
    def __init__(self, type_db: bool, word_datasets=None, emoji_datasets=None, emoticon_datasets=None):
        """
        DAO init
        @param type_db: True = relationalDB, False = MongoDB
        """
        if emoticon_datasets is None:
            emoticon_datasets = []
        if word_datasets is None:
            word_datasets = []
        if emoji_datasets is None:
            emoji_datasets = []

        self.dao_type = DaoMySQLDB() if type_db else DaoMongoDB(word_datasets, emoji_datasets, emoticon_datasets)

    def build_db(self):
        """
        Builds the DB
        """
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        self.dao_type.build_db(sentiments, get_sentiment_words(), get_sentiment_emoticons(), get_sentiment_emojis(), [])


if __name__ == '__main__':
    """ DAO """
    dao = Dao(True)
    dao.build_db()
