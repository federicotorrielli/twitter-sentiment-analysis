#!/usr/bin/python3
import sys

import pymysql
from pymysql import MySQLError
from toml import load
from src.file_manager import get_project_root, read_file
from src.slang import preparse_slang_toml_files, check_word_existence, preparse_standard_toml_files


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
        """ Relational DB auth """
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

    def build_db(self, sentiments, words, emoticons, emojis, twitter_paths):
        """
        Builds all tables of the relational db
        @param sentiments: list of sentiments
        @param words: set with words as keys and list of sentiments ids as values
        @param emoticons: set with polarity as keys and list of emoticons as values
        @param emojis: set with polarity as keys and list of emoji as values
        @param twitter_paths: list of file paths that contains tweets
        """
        print("Building DB")
        self.__drop_and_create_tables()
        print("\tAdding sentiments")
        self.__insert_sentiments(sentiments)
        print("\tAdding emoticons")
        self.__insert_emoticons(emoticons)
        print("\tAdding emojis")
        self.__insert_emojis(emojis)
        print("\tAdding words")
        self.__insert_words_sentiments(words)

        print("\tAdding tweets")
        assert len(sentiments) == len(twitter_paths)
        tweets_content = {sentiment: [] for sentiment in sentiments}
        for idx_sentiment, file_path in enumerate(twitter_paths):
            tweets_content[sentiments[idx_sentiment]] = read_file(file_path).splitlines()

        self.__insert_tweets_sentiments(tweets_content)

    def __drop_and_create_tables(self):
        """
        Drops tables if exist and creates them.
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                _execute_statement(cursor, "DROP TABLE IF EXISTS `labelled`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `belongs_to`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `word_in_tweet`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoticon_in_tweet`")
                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoji_in_tweet`")
                connection.commit()

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
                                           "`meaning` varchar(1024) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL DEFAULT '',"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoticon`")
                _execute_statement(cursor, "CREATE TABLE `emoticon` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoticon` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "DROP TABLE IF EXISTS `emoji`")
                _execute_statement(cursor, "CREATE TABLE `emoji` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoji` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
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
                                           "`perc_freq` float NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`sentiment_id`,`word_id`),"
                                           "FOREIGN KEY (`word_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `word_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`type` SET('word','emoji','emoticon') NOT NULL,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoticon_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`type` SET('word','emoji','emoticon') NOT NULL,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `emoticon` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoji_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`type` SET('word','emoji','emoticon') NOT NULL,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `emoji` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

    def __insert_sentiments(self, sentiments: [str]):
        """
        Inserts records into 'sentiment' table
        @param sentiments: list of sentiments
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

    def __insert_emoticons(self, polarity_emoticons: {}):
        """
        Inserts records into 'emoticon' table
        @param polarity_emoticons: set with polarity as keys and list of emoticons as values
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `emoticon` (`emoticon`, polarity) VALUES (%s, %s)"
                sql_params = []
                first_emoticon = True
                for index_polarity in polarity_emoticons:
                    for emoticon in polarity_emoticons[index_polarity]:
                        if not first_emoticon:
                            sql += ", (%s, %s)"
                        else:
                            first_emoticon = False
                        sql_params += [emoticon, index_polarity.lower()]
                        assert sql.count("%s") == len(sql_params)

                _execute_statement(cursor, sql, sql_params)
                connection.commit()

    def __insert_emojis(self, polarity_emojis: {}):
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
                sql_params = []
                first_emoji = True
                for index_polarity in polarity_emojis:
                    for emoji in polarity_emojis[index_polarity]:
                        if not first_emoji:
                            sql += ", (%s, %s)"
                        else:
                            first_emoji = False

                        sql_params += [emoji, index_polarity.lower()]
                        assert sql.count("%s") == len(sql_params)

                _execute_statement(cursor, sql, sql_params)
                connection.commit()

    def __insert_words_sentiments(self, sentiments_words: {}):
        """
        Inserts records into 'word' table and relations with 'sentiment' table
        @param sentiments_words: set with words as keys and list of sentiments ids as values
        """
        standard_toml_files = preparse_standard_toml_files()
        slang_toml_files = preparse_slang_toml_files()
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_word = "INSERT INTO `word` (`word`, `slang`, `meaning`) VALUES (%s, %s, %s) "
                sql_belongs_to = "INSERT INTO `belongs_to`(`sentiment_id`, `word_id`) VALUES (%s, %s)"
                sql_belongs_to_params = []

                first_insertion = True
                for word in sentiments_words:
                    # find word definition
                    is_slang = 0
                    definition = check_word_existence(word, standard_toml_files)
                    if definition == "":
                        definition = check_word_existence(word, slang_toml_files)
                        is_slang = definition != ""
                    definition = (definition[:1021] + '...') if len(definition) > 1024 else definition

                    _execute_statement(cursor, sql_word, [word, is_slang, definition])
                    id_word = cursor.lastrowid

                    for index, sentiment in enumerate(sentiments_words[word]):
                        if not first_insertion:
                            sql_belongs_to += ", (%s, %s)"
                        else:
                            first_insertion = False

                        sql_belongs_to_params += [sentiment + 1, id_word]
                        assert sql_belongs_to.count("%s") == len(sql_belongs_to_params)

                connection.commit()
                _execute_statement(cursor, sql_belongs_to, sql_belongs_to_params)
                connection.commit()

                print(f"{len(sentiments_words)} words added")
                print(f"{sum([len(sentiments_words[k]) for k in sentiments_words])} relations added")

    def __insert_tweets_sentiments(self, sentiments_tweets):
        """
        Inserts records into 'twitter_message' table and relations with 'sentiment' table
        @param sentiments_tweets:  set with sentiments as keys and list of tweets as values
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_insert_tweet = "INSERT INTO `twitter_message`(`tweet_content`) VALUES (%s) "
                sql_assign_sentiment = "INSERT INTO `labelled`(`sentiment_id`, `tweet_id`) VALUES (%s, %s)"
                sql_assign_sentiment_params = []
                first_sentiment_tweet = True

                for sentiment in sentiments_tweets:
                    id_sentiment = _execute_statement(cursor, f"SELECT id FROM `sentiment` "
                                                              f"WHERE `type` = \'{sentiment}\'").fetchone()[0]
                    assert id_sentiment >= 1

                    for tweet in sentiments_tweets[sentiment]:
                        _execute_statement(cursor, sql_insert_tweet, [tweet])
                        id_tweet = cursor.lastrowid

                        if not first_sentiment_tweet:
                            sql_assign_sentiment += ", (%s, %s)"
                        else:
                            first_sentiment_tweet = False
                        sql_assign_sentiment_params += [id_sentiment, id_tweet]

                        assert sql_assign_sentiment.count("%s") == len(sql_assign_sentiment_params)

                    connection.commit()
                _execute_statement(cursor, sql_assign_sentiment, sql_assign_sentiment_params)
                connection.commit()
