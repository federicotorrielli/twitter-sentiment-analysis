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
        self.sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

    def build_db(self):
        """
        Builds the DB
        """
        self.dao_type.build_db(self.sentiments, get_sentiment_words(), get_sentiment_emoticons(),
                               get_sentiment_emojis(), get_sentiment_tweets())

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
        self.dao_type.build_sentiments(self.sentiments, word_datasets, emoji_datasets, emoticon_datasets)
        # TODO: do it in dao_mysql_db

    def dump_definitions(self, definitions: dict, name: str):
        # TODO: do it in dao_mysql_db (see the mongo implementation)
        self.dao_type.dump_definitions(definitions, name)

    def get_definition(self, word: str, sentiment: str = "") -> str:
        # TODO: do it in dao_mysql_db (see the mongo implementation)
        return self.dao_type.get_definition(word, sentiment)

    def get_count(self, word: str):
        # TODO: do it in dao_mysql_db (see the mongo implementation)
        return self.dao_type.get_count(word)

    def get_popularity(self, word: str, count: dict = None):
        # TODO: do it in dao_mysql_db (see the mongo implementation)
        return self.dao_type.get_popularity(word, count)

    def push_result(self, word: str, count: dict, definition: str, popularity: dict):
        # TODO: do it in dao_mysql_db (see issue #10)
        return self.dao_type.push_result(word, count, definition, popularity)

    def get_result(self, word: str):
        # TODO: same
        return self.dao_type.get_result(word)
    
    def create_index(self, index: str, table: str):
        self.dao_type.create_index(index, table)
