#!/usr/bin/python3
import sys

import pymysql
from pymysql import MySQLError
from src.file_manager import get_project_root, read_file
from src.slang import (check_word_existence, preparse_slang_toml_files,
                       preparse_standard_toml_files)
from toml import load


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


def _executemany_statements(cursor, statement: str, params: [] = None):
    try:
        if params is None:
            cursor.execute(statement)
        else:
            cursor.executemany(statement, params)
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

        self.list_words = None
        self.list_new_lexicon = None
        self.list_emojis = None
        self.list_emoticons = None

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
                                     cursorclass=pymysql.cursors.DictCursor)
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
        print("\n\tAdding sentiments...")
        self.__insert_sentiments(sentiments)
        print("\n\tAdding emoticons...")
        self.__insert_emoticons(emoticons)
        print("\n\tAdding emojis...")
        self.__insert_emojis(emojis)
        print("\n\tAdding words...")
        self.__insert_words_sentiments(words)

        print("\n\tAdding tweets...")
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
                tables = ["labelled", "belongs_to",
                          "word_in_tweet", "emoticon_in_tweet", "emoji_in_tweet", "new_lexicon_in_tweet",
                          "hashtag_in_tweet",
                          "word_in_sentiment", "emoticon_in_sentiment", "emoji_in_sentiment",
                          "new_lexicon_in_sentiment", "hashtag_in_sentiment",
                          "sentiment", "word", "emoticon", "emoji", "new_lexicon", "hashtag", "twitter_message"]
                for table in tables:
                    _execute_statement(cursor, f"DROP TABLE IF EXISTS `{table}`")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `sentiment` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`type` varchar(32) NOT NULL UNIQUE,"
                                           "`perc_presence_lex_res` float NOT NULL DEFAULT -1,"
                                           "`perc_presence_twitter` float NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `twitter_message` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`tweet_content` varchar(280) NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `word` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`word` varchar(140) NOT NULL UNIQUE,"
                                           "`slang` BOOLEAN NOT NULL DEFAULT FALSE,"
                                           "`meaning` varchar(1024) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL DEFAULT '',"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `new_lexicon` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`token` varchar(140) NOT NULL,"
                                           "`slang` BOOLEAN NOT NULL DEFAULT FALSE,"
                                           "`meaning` varchar(1024) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL DEFAULT '',"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `hashtag` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`hashtag` varchar(140) NOT NULL UNIQUE,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoticon` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoticon` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoji` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`emoji` varchar(140) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL,"
                                           "`polarity` set('positive','negative','neutral','other') NOT NULL,"
                                           "PRIMARY KEY (`id`))")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `labelled` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`tweet_id` int NOT NULL,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `belongs_to` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`word_id` int NOT NULL,"
                                           "PRIMARY KEY(`sentiment_id`,`word_id`),"
                                           "FOREIGN KEY (`word_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `word_in_sentiment` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "`freq_perc` float NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`sentiment_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `hashtag_in_sentiment` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`sentiment_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `hashtag` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoji_in_sentiment` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`sentiment_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `emoji` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoticon_in_sentiment` ("
                                           "`sentiment_id` int NOT NULL,"
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`sentiment_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `emoticon` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `word_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `word` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `new_lexicon_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `new_lexicon` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `hashtag_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `hashtag` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoticon_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
                                           "PRIMARY KEY(`tweet_id`,`token_id`),"
                                           "FOREIGN KEY (`token_id`) REFERENCES `emoticon` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE,"
                                           "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`) "
                                           "ON DELETE CASCADE ON UPDATE CASCADE)")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `emoji_in_tweet` ("
                                           "`tweet_id` int NOT NULL, "
                                           "`token_id` int NOT NULL,"
                                           "`count` int NOT NULL DEFAULT -1,"
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
                print("\n\tBuilding `sentiment` table...")
                _executemany_statements(cursor, sql, sentiments)
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
                for index_polarity in polarity_emoticons:
                    for emoticon in polarity_emoticons[index_polarity]:
                        sql_params += [(emoticon, index_polarity.lower())]
                print("\n\tBuilding `emoticon` table...")
                _executemany_statements(cursor, sql, sql_params)
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
                for index_polarity in polarity_emojis:
                    for emoji in polarity_emojis[index_polarity]:
                        sql_params += [(emoji, index_polarity.lower())]
                print("\n\tBuilding `emoji` table...")
                _executemany_statements(cursor, sql, sql_params)
                connection.commit()

    def __insert_words(self, words: []):
        """
        Inserts records into 'word' table
        @param words: list of words
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_word = "INSERT INTO `word` (`word`) VALUES (%s) "
                print("\n\tBuilding `word` table...")
                _executemany_statements(cursor, sql_word, words)
                connection.commit()

    def __insert_tweets(self, tweets: []):
        """
        Inserts records into 'word' table
        @param tweets: list of tweets
        """
        # TODO: make one func easy_executemany(query: str, params: list(tuple()), message: str)
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_word = "INSERT INTO `twitter_message`(`tweet_content`) VALUES (%s)"
                print("\n\tBuilding `twitter_message` table...")
                _executemany_statements(cursor, sql_word, tweets)
                connection.commit()

    def __insert_words_sentiments(self, sentiments_words: {}):
        """
        Inserts records into 'word' table and relations with 'sentiment' table
        @param sentiments_words: set with words as keys and list of sentiments ids as values
        """
        self.__insert_words([word for word in sentiments_words])
        if self.list_words is None:
            self.list_words = self.get_tokens("word")

        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql_belongs_to = "INSERT INTO `belongs_to`(`sentiment_id`, `word_id`) VALUES (%s, %s)"
                sql_belongs_to_params = []

                for word in sentiments_words:
                    for index, sentiment in enumerate(sentiments_words[word]):
                        # TODO: change sentiment from int to string
                        sql_belongs_to_params += [(sentiment + 1, self.list_words[word])]

                print("\n\tBuilding `belongs_to` table...")
                _executemany_statements(cursor, sql_belongs_to, sql_belongs_to_params)
                connection.commit()

    def dump_definitions(self):
        """
        Dumps all the definitions in 'word' table
        """
        standard_toml_files = preparse_standard_toml_files()
        slang_toml_files = preparse_slang_toml_files()
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                words = {}
                tables = ["word", "new_lexicon"]
                for table in tables:
                    field = "word" if table == "word" else "token"
                    res = _execute_statement(cursor, f"SELECT `id`, `{field}` FROM `{table}`").fetchall()
                    for t in res:
                        words[t[f"{field}"]] = t["id"]

                    sql_update_words = f"UPDATE `{table}` SET `meaning` = %s, `slang` = %s WHERE `id` = %s"
                    sql_params = []

                    for word in words:
                        # find word definition
                        is_slang = 0
                        definition = check_word_existence(word, standard_toml_files)
                        if definition == "":
                            definition = check_word_existence(word, slang_toml_files)
                            is_slang = definition != ""
                            definition = "NOTHING FOUND" if definition == "" else definition

                        definition = (definition[:1021] + '...') if len(definition) > 1024 else definition
                        sql_params += [(definition, is_slang, words[word])]

                    print(f"\n\tAdding `{table}` meanings...")
                    _executemany_statements(cursor, sql_update_words, sql_params)
                    connection.commit()

    def __insert_tweets_sentiments(self, sentiments_tweets):
        """
        Inserts records into 'twitter_message' table and relations with 'sentiment' table
        @param sentiments_tweets: set with sentiments as keys and list of tweets as values
        """
        # TODO: optimize func
        all_tweets = [tweet for sentiment in sentiments_tweets for tweet in sentiments_tweets[sentiment]]
        self.__insert_tweets(all_tweets)
        list_tweets = self.get_tweets_list()
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `labelled`(`sentiment_id`, `tweet_id`) VALUES (%s, %s)"
                # sql_params = []

                for sentiment in sentiments_tweets:
                    sql_params = []
                    cursor = _execute_statement(cursor, f"SELECT `id` FROM `sentiment` "
                                                        f"WHERE `type` = \'{sentiment}\'")
                    id_sentiment = cursor.fetchone()["id"] if cursor is not None else -1
                    assert id_sentiment >= 1
                    for tweet in sentiments_tweets[sentiment]:
                        sql_params += [(id_sentiment, list_tweets[tweet])]
                    print(f"\n\tBuilding ({sentiment}) `labelled` table...")
                    # print(f"\n\tBuilding `labelled` table...")
                    _executemany_statements(cursor, sql, sql_params)
                    connection.commit()

    def get_definition(self, word: str) -> str:
        """
        Given a word it returns the correct definition, given that
        it is already stored on the database
        @param word: the word to search the definition for
        @return: the definition of the word
        """
        definition = "NOTHING FOUND"
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "SELECT `meaning` FROM `word` WHERE `word` = %s"
                res = _execute_statement(cursor, sql, [word]).fetchone()
                definition = res["meaning"] if res else definition
        return definition

    def get_count(self, word: str) -> dict:
        """
        Given the word, it returns the count of it, for every sentiment
        @param word: the word or emoji you are trying to find
        @return: the number of times that word appeared for every sentiment (a dict)
        """
        counts = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "SELECT `type`, `count` " \
                      "FROM `sentiment` LEFT JOIN `word_in_sentiment` ON `sentiment_id` = `id` " \
                      "WHERE `token_id` = (SELECT `id` FROM `word` WHERE `word` = %s)"
                cursor = _execute_statement(cursor, sql, [word])
                counts = {t["type"]: t["count"] for t in cursor.fetchall().fetchall() if cursor is not None is not None}
        return counts

    def __get_word_numbers(self, sentiment: str):
        """
        Count how many words related to sentiment
        @param sentiment:
        @return: words count
        """
        counts = -1
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "SELECT COUNT(*) AS c FROM `belongs_to` " \
                      "WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment])
                res = cursor.fetchone()
                counts = res["c"] if res else counts
        return counts

    def get_popularity(self, word: str, count: dict = None, sentiment_word_numbers: dict = None) -> dict:
        """
        Given the word it returns a dict containing all the
        popularity percentages for every sentiment. If a count (dict)
        is not given, it will take more time because it must reach the
        server and query the database every time
        @param word: the word to get the popularity from
        @param count: the count dict, see get_count(...)
        @param sentiment_word_numbers: dict that contains words counting per sentiment
        @return: a dict of all the percentages for every sentiment
        """
        if count is None:
            count = self.get_count(word)

        popularities = {}
        if sentiment_word_numbers is None:
            sentiment_word_numbers = {}
        for sentiment in count:
            if sentiment not in sentiment_word_numbers:
                sentiment_word_numbers[sentiment] = self.__get_word_numbers(sentiment)
            # popularities[sentiment] = count[sentiment] / sentiment_word_numbers[sentiment]
            popularities[sentiment] = self.__calculate_popularity(count[sentiment], self.__get_word_numbers(sentiment))
        return popularities

    def __calculate_popularity(self, count, sentiment_tot):
        return round(count / sentiment_tot, 4)

    def get_tweets_list(self, sentiment: str = ""):
        """
        Gets the dict of tweets of type {sentiment_number: "tweet"}
        where the number is monotonic and generated during the building
        of the database
        @param sentiment:
        @return: dict {id_tweet: "tweet"}
        """
        tweets = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "SELECT `id`, `tweet_content` FROM `twitter_message`"
                if sentiment != "":
                    sql += "LEFT JOIN `labelled` on `id` = `tweet_id` " \
                           "WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment] if sentiment != "" else [])
                tweets = {t["tweet_content"]: t["id"] for t in cursor.fetchall() if cursor is not None}
        return tweets

    def get_tweets(self, sentiment: str = ""):
        """
        Gets the dict of tweets of type {sentiment_number: "tweet"}
        where the number is monotonic and generated during the building
        of the database
        @param sentiment:
        @return: dict {id_tweet: "tweet"}
        """
        tweets = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "SELECT `id`, `tweet_content` " \
                      "FROM `twitter_message` LEFT JOIN `labelled` on `id` = `tweet_id` " \
                      "WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment] if sentiment != "" else [])
                tweets = {t["id"]: t["tweet_content"] for t in cursor.fetchall() if cursor is not None}
        return tweets

    def get_tokens(self, token_type: str):
        """
        Gets all tokens of type
        @param token_type: possibile values "word", "emoji" or "emoticon"
        @return: dict {"token": id}
        """
        token_type = token_type.lower()
        tokens = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                field = "token" if token_type == "new_lexicon" else token_type
                sql = f"SELECT `id`, `{field}` FROM `{token_type}` "
                cursor = _execute_statement(cursor, sql)
                tokens = {t[f"{field}"]: t["id"] for t in cursor.fetchall() if cursor is not None}
        return tokens

    def add_tweets_tokens(self, tweets_tokens: {}):
        """
        Inserts the connection between tweets and tokens
        @param tweets_tokens: {tweet_id: [tokens]...}
        """
        # TODO: optimize func
        if self.list_words is None:
            self.list_words = self.get_tokens("word")
        if self.list_new_lexicon is None:
            self.list_new_lexicon = self.get_tokens("new_lexicon")
        if self.list_emojis is None:
            self.list_emojis = self.get_tokens("emoji")
        if self.list_emoticons is None:
            self.list_emoticons = self.get_tokens("emoticon")

        # data_token = {tweet_id: {token_id: count,...},...}
        data_word = {}
        data_new = {}
        data_emoji = {}
        data_emoticon = {}

        for tweet_id in tweets_tokens:
            for token in tweets_tokens[tweet_id]:
                if token in self.list_words:
                    if tweet_id not in data_word or self.list_words[token] not in data_word[tweet_id]:
                        if tweet_id not in data_word:
                            data_word[tweet_id] = {}
                        data_word[tweet_id][self.list_words[token]] = 1
                    else:
                        data_word[tweet_id][self.list_words[token]] += 1

                elif token in self.list_emojis:
                    if tweet_id not in data_emoji or self.list_emojis[token] not in data_emoji[tweet_id]:
                        if tweet_id not in data_emoji:
                            data_emoji[tweet_id] = {}
                        data_emoji[tweet_id][self.list_emojis[token]] = 1
                    else:
                        data_emoji[tweet_id][self.list_emojis[token]] += 1

                elif token in self.list_emoticons:
                    if tweet_id not in data_emoticon or self.list_emoticons[token] not in data_emoticon[tweet_id]:
                        if tweet_id not in data_emoticon:
                            data_emoticon[tweet_id] = {}
                        data_emoticon[tweet_id][self.list_emoticons[token]] = 1
                    else:
                        data_emoticon[tweet_id][self.list_emoticons[token]] += 1

                elif token in self.list_new_lexicon:
                    if tweet_id not in data_new or self.list_new_lexicon[token] not in data_new[tweet_id]:
                        if tweet_id not in data_new:
                            data_new[tweet_id] = {}
                        data_new[tweet_id][self.list_new_lexicon[token]] = 1
                    else:
                        data_new[tweet_id][self.list_new_lexicon[token]] += 1

        data_to_insert = {
            "word": data_word,
            "new_lexicon": data_new,
            "emoji": data_emoji,
            "emoticon": data_emoticon
        }
        self.insert_tweets_tokens(data_to_insert)

    def insert_tweets_tokens(self, data_to_insert: {}):
        """
        Inserts the connection between tweets and tokens
        @param data_to_insert: {token_type: {tweet_id: {token_id: count,...},...},...}
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                for token_type in data_to_insert:
                    sql = f"INSERT INTO `{token_type}_in_tweet` " \
                          f"(`tweet_id`, `token_id`, `count`) VALUES (%s, %s, %s)"
                    sql_params = []
                    if len(data_to_insert[token_type]) > 0:
                        for tweet_id in data_to_insert[token_type]:
                            for token_id in data_to_insert[token_type][tweet_id]:
                                sql_params += [(tweet_id, token_id, data_to_insert[token_type][tweet_id][token_id])]
                        print(f"\n\tBuilding `{token_type}_in_tweet` table...")
                        _executemany_statements(cursor, sql, sql_params)
                        connection.commit()

    def get_counts(self, sentiment: str, token_type: str):
        """
        Gets the dict of counts of type {item: count}
        where the number is monotonic and generated during the building
        of the database
        @param sentiment: document name
        @param token_type: token category
        @return: dict {item: count}
        """
        return self.__get_counts_or_frequencies(True, sentiment, token_type)

    def get_frequencies(self, sentiment: str, token_type: str):
        """
        Gets the dict of frequencies of type {item: frequency}
        where the number is monotonic and generated during the building
        of the database
        @param sentiment: document name
        @param token_type: token category
        @return: dict {item: count}
        """
        return self.__get_counts_or_frequencies(False, sentiment, token_type)

    def __get_counts_or_frequencies(self, counts: bool, sentiment: str, token_type: str):
        """
        Gets the dict of counts/frequencies of type {item: count/frequency}
        where the number is monotonic and generated during the building
        of the database
        @param counts: get tokens' counter if true, otherwise tokens' frequency
        @param sentiment: document name
        @param token_type: token category
        @return: dict {item: count}
        """
        param = "count" if counts else "freq_perc"
        data = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = f"SELECT `{token_type}`, `{param}` " \
                      f"FROM `{token_type}` LEFT JOIN `{token_type}_in_sentiment` on `id` = `token_id` " \
                      f"WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment])
                data = {t[f"{token_type}"]: t[f"{param}"] for t in cursor.fetchall() if cursor is not None}
        return data

    def insert_hashtags(self, hashtags: list):
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `hashtag`(`hashtag`) VALUES (%s)"
                print("\n\tBuilding `hashtag` table...")
                _executemany_statements(cursor, sql, hashtags)
                connection.commit()

    def build_sentiments(self, sentiments, word_datasets, emoji_datasets, emoticon_datasets, hashtag_datasets):
        """
        Puts in the db the different datasets built in natural
        @param sentiments: array of sentiments
        @param word_datasets: a list of dicts for every word_frequency sentiment
        @param emoji_datasets: a list of dicts for every emoji_frequency sentiment
        @param emoticon_datasets: a list of dicts for every emoticon_frequency sentiment
        """
        # TODO: optimize func
        if self.list_words is None:
            self.list_words = self.get_tokens("word")
        if self.list_emojis is None:
            self.list_emojis = self.get_tokens("emoji")
        if self.list_emoticons is None:
            self.list_emoticons = self.get_tokens("emoticon")

        hashtags_to_insert = [hashtag for hashtags in hashtag_datasets for hashtag in hashtags]
        self.insert_hashtags(list(dict.fromkeys(hashtags_to_insert)))
        list_hashtag = self.get_tokens("hashtag")
        sentiments_count = {sentiment: self.__get_word_numbers(sentiment) for sentiment in sentiments}

        for index, sentiment in enumerate(sentiments):
            with self.__connect_db() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SET NAMES utf8mb4')
                    cursor.execute("SET CHARACTER SET utf8mb4")
                    cursor.execute("SET character_set_connection=utf8mb4")

                    cursor = _execute_statement(cursor, f"SELECT `id` FROM `sentiment` "
                                                        f"WHERE `type` = \'{sentiment}\'")
                    id_sentiment = cursor.fetchone()["id"] if cursor is not None else -1
                    assert id_sentiment >= 1

                    sql_insert = "INSERT INTO `word_in_sentiment` (`count`, `sentiment_id`, `token_id`, `freq_perc`) " \
                                 "VALUES (%s, %s, %s, %s)"
                    sql_params = []
                    for word in word_datasets[index]:
                        if word in self.list_words:
                            sql_params += [(word_datasets[index][word],
                                            id_sentiment,
                                            self.list_words[word],
                                            self.__calculate_popularity(word_datasets[index][word],
                                                                        sentiments_count[sentiment]))]
                    if sql_params is not []:
                        print("\n\tBuilding `word_in_sentiment` table...")
                        _executemany_statements(cursor, sql_insert, sql_params)
                        connection.commit()

                    sql_insert = "INSERT INTO `emoji_in_sentiment` (`count`, `sentiment_id`, `token_id`) " \
                                 "VALUES (%s, %s, %s)"
                    sql_params = []
                    for emoji in emoji_datasets[index]:
                        if emoji in self.list_emojis:
                            sql_params += [(emoji_datasets[index][emoji], id_sentiment, self.list_emojis[emoji])]
                    if sql_params is not []:
                        print("\n\tBuilding `emoji_in_sentiment` table...")
                        _executemany_statements(cursor, sql_insert, sql_params)
                        connection.commit()

                    sql_insert = "INSERT INTO `emoticon_in_sentiment` (`count`, `sentiment_id`, `token_id`) " \
                                 "VALUES (%s, %s, %s)"
                    sql_params = []
                    for emoticon in emoticon_datasets[index]:
                        if emoticon in self.list_emoticons:
                            sql_params += [(emoticon_datasets[index][emoticon], id_sentiment, self.list_emoticons[emoticon])]
                    if sql_params is not []:
                        print("\n\tBuilding `emoticon_in_sentiment` table...")
                        _executemany_statements(cursor, sql_insert, sql_params)
                        connection.commit()

                    sql_insert = "INSERT INTO `hashtag_in_sentiment` (`count`, `sentiment_id`, `token_id`) " \
                                 "VALUES (%s, %s, %s)"
                    sql_params = []
                    for hashtag in hashtag_datasets[index]:
                        if hashtag in list_hashtag:
                            sql_params += [(hashtag_datasets[index][hashtag], id_sentiment, list_hashtag[hashtag])]
                    if sql_params is not []:
                        print("\n\tBuilding `hashtag_in_sentiment` table...")
                        _executemany_statements(cursor, sql_insert, sql_params)
                        connection.commit()

    def get_result(self, word: str) -> dict:
        """
        Gets a result dict, given the word (see push_results(...))
        @param word: the word to get the result dict from
        @return: the result dict
        """
        word = word.lower()
        result = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "SELECT * FROM `word` WHERE `word` = %s"
                res = _execute_statement(cursor, sql, [word]).fetchone()
                if res:
                    result["word"] = word
                    result["id"] = res["id"]
                    result["slang"] = True if res["slang"] == 0 else False
                    result["definition"] = res["meaning"]
                    result["count"] = {}
                    result["popularity"] = {}

                    sql = "SELECT `sentiment_id`, `type`, `count`, `freq_perc` " \
                          "FROM `word_in_sentiment` RIGHT JOIN `sentiment` ON `sentiment_id` = `id` " \
                          "WHERE `token_id` = %s " \
                          "GROUP BY `sentiment_id`"
                    res = _execute_statement(cursor, sql, [result["id"]])
                    if res is not None:
                        for t in res.fetchall():
                            result["count"][t["type"]] = t["count"]
                            result["popularity"][t["type"]] = t["freq_perc"]

        return result

    def add_all_sentiment_perc(self, sentiment_percentages: dict):
        """
        Insert all sentiment percentages
        @param sentiment_percentages: {'sentiment': (perc_presence_lex_res, perc_presence_twitter)}
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "UPDATE `sentiment` " \
                      "SET `perc_presence_lex_res` = %s, `perc_presence_twitter` = %s " \
                      "WHERE `type` = %s"
                sql_params = []
                for sentiment in sentiment_percentages:
                    perc_presence_lex_res, perc_presence_twitter = sentiment_percentages[sentiment]
                    sql_params += [(perc_presence_lex_res, perc_presence_twitter, sentiment)]
                print("\n\tAdding `perc_presence_lex_res` and `perc_presence_twitter` to `sentiment` table...")
                _executemany_statements(cursor, sql, sql_params)
                connection.commit()

    def get_sentiment_percentages(self):
        """
        Gets sentiments' percentages
        @return: {"sentiment": (perc_presence_lex_res, perc_presence_twitter)}
        """
        sentiment_percentages = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "SELECT `type`, `perc_presence_lex_res`, `perc_presence_twitter` FROM `sentiment`"

                res = _execute_statement(cursor, sql)
                sentiment_percentages = {t["type"]: (t["perc_presence_lex_res"], t["perc_presence_twitter"])
                                         for t in res.fetchall() if res is not None}
        return sentiment_percentages

    def dump_new_lexicon(self, wordlist: []):
        """
        Dumps on the DBs the new words from twitter that are not present in the
        pre-existent wordlist given by the project
        @param wordlist:
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `new_lexicon` (`token`) VALUES (%s)"

                print("\n\tBuilding `new_lexicon` table...")
                _executemany_statements(cursor, sql, wordlist)
                connection.commit()


if __name__ == '__main__':
    """ 
    Tests
    """
    db = DaoMySQLDB()
    print(db.get_result("hello"))
