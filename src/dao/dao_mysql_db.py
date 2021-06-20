#!/usr/bin/python3
import sys
import pymysql
from pymysql import MySQLError
from toml import load
from src.datasets_manager import get_sentiment_words, get_sentiment_emojis, get_sentiment_emoticons
from src.file_manager import get_project_root


def _execute_statement(cursor, statement: str, params: [] = None):
    try:
        if params is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, params)
    except MySQLError as error:
        print(f"Error: {error}", file=sys.stderr)
    finally:
        print(f"\tstatement: {cursor._last_executed}")

    return cursor


class DaoMySQLDB:
    def __init__(self):
        """ Relational DB """
        auth = load(f"{get_project_root()}/auth/auth.toml")
        self.user = auth.get("user")
        self.pwd = auth.get("password")
        self.host = auth.get("host")
        self.db = auth.get("db")

    def __connect_db(self):
        """
        Connection to the Relational DB.
        @return: the connection to the Relational DB
        """
        connection = pymysql.connect(host=self.host,
                                     port=3306,
                                     user=self.user,
                                     passwd=self.pwd,
                                     db=self.db,
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.SSCursor)
        return connection

    def build_db(self, sentiments, words, emoticons, emojis, tweets):
        """
        Builds all tables of the relational db
        """
        print("Building DB")
        self.__drop_and_create_tables()
        print("Adding sentiments")
        self.insert_sentiments(sentiments)
        # print("Adding words")
        # self.__insert_words(words)
        # print("Adding emoticons")
        # self.__insert_emoticons(emoticons)
        # print("Adding emojis")
        # self.__insert_emojis(emojis)

        # print("Adding tweets")
        # self.insert_tweets(tweets)

    def __drop_and_create_tables(self):
        """
        Drops tables if exist and creates them.
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                _execute_statement(cursor, "DROP TABLE IF EXISTS `labelled`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `belongs_to`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `in_tweet`")

                _execute_statement(cursor, "DROP TABLE IF EXISTS `sentiment`")
                _execute_statement(cursor, "CREATE TABLE `sentiment` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`type` varchar(32) NOT NULL UNIQUE,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `twitter_message`")
                _execute_statement(cursor, "CREATE TABLE `twitter_message` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`tweet_content` varchar(280) NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `word`")
                _execute_statement(cursor, "CREATE TABLE `word` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`word` varchar(140) NOT NULL UNIQUE,"
                                           "`slang` BOOLEAN NOT NULL DEFAULT FALSE,"
                                           "`meaning` varchar(512) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`count` int NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoticon`")
                _execute_statement(cursor, "CREATE TABLE `emoticon` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoticon` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "`count` int NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoji`")
                _execute_statement(cursor, "CREATE TABLE `emoji` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoji` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "`meaning` varchar(512) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`count` int NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `labelled` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`tweet_id` int NOT NULL,"
                                           "PRIMARY KEY(`sentiment_id`,`tweet_id`),"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `belongs_to` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`word_id` int NOT NULL,"
                                           "`perc_freq` float NOT NULL DEFAULT 0,"
                                           "PRIMARY KEY(`sentiment_id`,`word_id`),"
                                           "FOREIGN KEY (`word_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`type` SET('WORD','EMOJI','EMOTICON') NOT NULL,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

    def insert_sentiments(self, sentiments: [str]):
        """
        Inserts records into 'sentiment' table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `sentiment` (`type`) VALUES (%s)"
                for s in sentiments:
                    _execute_statement(cursor, sql, s)
                connection.commit()

    def insert_words(self, word_sentiments: {}):
        """
        Inserts records into 'word' table
        @param word_sentiments: set with words as keys and list of sentiments ids as values
        """
        # TODO: field 'slang' = 1 when needed
        # TODO: add words meaning
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_word = "INSERT INTO `word` (`word`) VALUES "
                sql_word_add = "(%s)"
                sql_word_params = []
                sql_belongs_to = "INSERT INTO `belongs_to`(`sentiment_id`, `word_id`) VALUES "
                sql_belongs_to_add = "(%s, %s)"
                sql_belongs_to_params = []

                first_insertion = True
                id_word = 0
                for word in word_sentiments:
                    id_word += 1
                    if not first_insertion:
                        sql_word += ", "
                    sql_word += sql_word_add
                    sql_word_params += [word]

                    for index, sentiment in enumerate(word_sentiments[word]):
                        if not first_insertion:
                            sql_belongs_to += ", "

                        sql_belongs_to += sql_belongs_to_add
                        sql_belongs_to_params += [word_sentiments[word][index] + 1, id_word]
                        first_insertion = False

                _execute_statement(cursor, sql_word, sql_word_params)
                connection.commit()
                _execute_statement(cursor, sql_belongs_to, sql_belongs_to_params)
                connection.commit()

                print(f"{len(word_sentiments)} words added")
                print(f"{sum([len(word_sentiments[k]) for k in word_sentiments])} relations added")

    def insert_emoticons(self, polarity_emoticons: {}):
        """
        Inserts records into 'emoticon' table
        @param polarity_emoticons: set with polarity as keys and list of emoticons as values
        """
        # TODO: add emoji meaning
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `emoticon` (`emoticon`, polarity) VALUES (%s, %s)"
                for index in polarity_emoticons:
                    for emoticon in polarity_emoticons[index]:
                        if emoticon == 'X-D':
                            print("")
                        _execute_statement(cursor, sql, [emoticon, index])
                connection.commit()

    def insert_emojis(self, polarity_emojis: {}):
        """
        Inserts records into 'emoji' table
        @param polarity_emojis: set with polarity as keys and list of emoji as values
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `emoji` (`emoji`, polarity) VALUES (%s, %s)"
                for index in polarity_emojis:
                    for emoji in polarity_emojis[index]:
                        _execute_statement(cursor, sql, [emoji, index])
                connection.commit()

    def insert_tweets(self, tweets_content: [str]):
        """
        Inserts records into 'twitter_message' table
        @param tweets_content: list of tweets' content
        """
        # TODO: tweets, definitions, counts, percs
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_insert_tweet = "INSERT INTO `twitter_message`(`tweet_content`) VALUES "
                sql_insert_tweet_params = []
                tweets_sentiment = {}
                first_tweet = True
                for sentiment in tweets_content:
                    sentiment_id = _execute_statement(cursor, f"SELECT id FROM `sentiment` "
                                                              f"WHERE `type` = \'{sentiment}\'").fetchone()
                    # TODO: if not exist, next sentiment
                    for tweet in tweets_content[sentiment]:
                        if not first_tweet:
                            sql_insert_tweet += ", "
                        first_tweet = False

                        # TODO: check if the tweet already exists
                        sql_insert_tweet += "(%s)"
                        sql_insert_tweet_params += [tweet]
                        tweets_sentiment[cursor.lastrowid] = sentiment_id['id']

                _execute_statement(cursor, sql_insert_tweet, sql_insert_tweet_params)
                connection.commit()

                # adding relations between 'twitter_message' and 'sentiment'
                self.insert_sentiment_to_tweet(tweets_sentiment)

    def insert_sentiment_to_tweet(self, tweets_sentiment: {}):
        """
        Inserts records into 'labelled' table
        @param tweets_sentiment: dict(tweet_id:sentiment:id)
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql_assign_sentiment = "INSERT INTO `labelled`(`sentiment_id`, `tweet_id`) VALUES "
                sql_assign_sentiment_params = []
                first_tweet = True
                for tweet_id in tweets_sentiment:
                    if not first_tweet:
                        sql_assign_sentiment += ", "
                    first_tweet = False
                    sql_assign_sentiment += "(%s, %s)"
                    sql_assign_sentiment_params += [tweets_sentiment[tweet_id], tweet_id]
                _execute_statement(cursor, sql_assign_sentiment, [sql_assign_sentiment_params])
                connection.commit()

    def _easy_statement_exec(self, statement: str, params: [str]):
        """
        Given a MySQL statement and the parameters eventually needed, it execs the statement
        @param statement: MySQL statement
        @param params: parameters eventually needed inside the statement
        @return: cursor
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")
                cursor = _execute_statement(cursor, statement, params)
                connection.commit()
        return cursor
