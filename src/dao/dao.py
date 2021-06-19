#!/usr/bin/python3
from src.dao.dao_mongo_db import DaoMongoDB
from src.dao.dao_mysql_db import DaoMySQLDB
from src.datasets_manager import get_sentiment_words, get_sentiment_emoticons, get_sentiment_emojis, \
    get_sentiment_tweets


class Dao:
    def __init__(self, type_db: bool):
        """
        DAO init
        @param type_db: True = relationalDB, False = MongoDB
        """
        if type_db:
            self.dao_type = DaoMySQLDB()
        else:
            self.dao_type = DaoMongoDB()

    def build_db(self):
        """
        Builds the DB
        """
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        self.dao_type.build_db(sentiments, get_sentiment_words(), get_sentiment_emoticons(), get_sentiment_emojis(),
                               get_sentiment_tweets())

    def get_document(self, collection_name):
        """
        Gets the dict of tweets of type {sentiment_number: "tweet"}
        where the number is monotonic and generated during the building
        of the database
        """
        return self.dao_type.get_document(collection_name)
        # TODO: do it in dao_mysql_db

    def build_sentiments(self, word_datasets, emoji_datasets, emoticon_datasets):
        """
        Puts in the db the different datasets built in natural
        @param word_datasets:
        @param emoji_datasets:
        @param emoticon_datasets:
        """
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        self.dao_type.build_sentiments(sentiments, word_datasets, emoji_datasets, emoticon_datasets)
        # TODO: do it in dao_mysql_db

    def dump_definitions(self, definitions: dict, name: str):
        self.dao_type.dump_definitions(definitions, name)
        # TODO: do it in dao_mysql_db

    def get_definition(self, word: str, sentiment: str = "") -> str:
        # TODO: do it in dao_mysql_db
        return self.dao_type.get_definition(word, sentiment)
