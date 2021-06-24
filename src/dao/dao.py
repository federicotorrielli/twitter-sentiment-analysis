#!/usr/bin/python3
from timeit import default_timer as timer
from src.dao.dao_mongo_db import DaoMongoDB
from src.dao.dao_mysql_db import DaoMySQLDB
from src.datasets_manager import (get_sentiment_emojis,
                                  get_sentiment_emoticons,
                                  get_sentiment_tweets, get_sentiment_words)


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
        self.type_db = type_db
        self.sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

    def is_mongodb(self):
        return not self.type_db

    def build_db(self):
        """
        Builds the DB
        """
        self.dao_type.build_db(self.sentiments, get_sentiment_words(), get_sentiment_emoticons(),
                               get_sentiment_emojis(), get_sentiment_tweets())

    def get_tweets(self, collection_name):
        """
        Gets the dict of tweets of type {sentiment_number: "tweet"}
        where the number is monotonic and generated during the building
        of the database
        @param collection_name: document name
        @return: dict {sentiment_number: "tweet"}
        """
        return self.dao_type.get_tweets(collection_name)

    def get_counts(self, collection_name, token_type: str = ""):
        """
        Gets the dict of count of type {item: count}
        where the number is monotonic and generated during the building
        of the database
        @param collection_name: document name
        @param token_type: token category
        @return: dict {item: count}
        """
        return self.dao_type.get_counts(collection_name, token_type)

    def build_sentiments(self, word_datasets, emoji_datasets, emoticon_datasets):
        """
        Puts in the db the different datasets built in natural
        @param word_datasets: a list of dicts for every word_count sentiment
        @param emoji_datasets: a list of dicts for every emoji_count sentiment
        @param emoticon_datasets: a list of dicts for every emoticon_count sentiment
        """
        self.dao_type.build_sentiments(self.sentiments, word_datasets, emoji_datasets, emoticon_datasets)

    def dump_definitions(self, definitions: dict = {}, name: str = ""):
        """
        Dumps all the definitions in the correct @name table/document
        @param definitions: a dict containing tuples {word: definition}, not needed in MySQL
        @param name: the name of the table/document to put it in, not needed in MySQL
        """
        if self.type_db:
            self.dao_type.dump_definitions()
        else:
            self.dao_type.dump_definitions(definitions, name)

    def get_definition(self, word: str, sentiment: str = "") -> str:
        """
        Given a word it returns the correct definition, given that
        it is already stored on the database
        @param word: the word to search the definition for
        @param sentiment: (optional) the sentiment to search it for
        @return: the definition of the word
        """
        if self.type_db:
            return self.dao_type.get_definition(word.lower())
        else:
            return self.dao_type.get_definition(word.lower(), sentiment.lower())

    def get_count(self, word: str) -> dict:
        """
        Given the word, it returns the count of it, for every sentiment
        @param word: the word or emoji you are trying to find
        @return: the number of times that word appeared for every sentiment (a dict)
        """
        return self.dao_type.get_count(word.lower())

    def get_popularity(self, word: str, count: dict = None) -> dict:
        """
        Given the word it returns a dict containing all the
        popularity percentages for every sentiment. If a count (dict)
        is not given, it will take more time because it must reach the
        server and query the database every time
        @param word: the word to get the popularity from
        @param count: the count dict, see get_count(...)
        @return: a dict of all the percentages for every sentiment
        """
        if self.type_db:
            return self.dao_type.get_popularity(word.lower(), count)
        else:
            return self.dao_type.get_popularity(word.lower(), count)

    def push_result(self, word: str, count: dict, definition: str, popularity: dict):
        """
        Pushes a single result to the "result" table/document
        @param word: the word to push
        @param count: the count dict (see get_count(...))
        @param definition: the definition string (see get_definition(...))
        @param popularity: the popularity dict (see get_popularity(...))
        """
        if not self.type_db:
            self.dao_type.push_result(word.lower(), count, definition, popularity)

    def push_results(self, result_list: []):
        """
        Pushes a dict of results to the "result" table/document.
        This is a much faster way to process the result and push
        it to the database
        @param result_list: [...{}...]
        """
        if not self.type_db:
            self.dao_type.push_results(result_list)

    def get_result(self, word: str) -> dict:
        """
        Gets a result dict, given the word (see push_results(...))
        @param word: the word to get the result dict from
        @return: the result dict
        """
        return self.dao_type.get_result(word.lower())

    def create_index(self, index: str, table: str):
        """
        Creates an index on the attribute "index" on the table "table"
        @param index: The index / attribute to create
        @param table: The table / document to put the index on
        """
        self.dao_type.create_index(index, table)

    def add_tweets_tokens(self, tweets_tokens: dict):
        """
        Inserts the connection between tweets and tokens. Used only in relational db
        @param tweets_tokens: {tweet_id: [tokens]...}
        """
        if self.type_db:
            self.dao_type.add_tweets_tokens(tweets_tokens)


if __name__ == '__main__':
    """ 
    Tests
    """
    word = "Hello"
    db_type = True  # True value: MySQL
    start = timer()
    db = Dao(db_type)
    db.build_db()
    db.dump_definitions()
    # print(db.get_tokens("word"))
    # print(db.get_tokens("emoji"))
    # print(db.get_tokens("emoticon"))
    # print(db.get_definition(word))
    # print(db.get_count(word))
    # print(db.get_popularity("Hello"))
    end = timer()
    print(f"Done in {end - start} seconds")

    # db_type = False  # False value: MongoDB
    # start = timer()
    # db = Dao(db_type)
    # # db.build_db()
    # print(db.get_result("hello"))
    # # print(db.get_definition(word))
    # # print(db.get_count(word))
    # # print(db.get_popularity(word))
    # end = timer()
    # print(f"Done in {end - start} seconds")
