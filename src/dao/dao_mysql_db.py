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
                tables = ["labelled", "belongs_to", "word_in_tweet", "emoticon_in_tweet", "emoji_in_tweet",
                          "word_in_sentiment", "emoticon_in_sentiment", "emoji_in_sentiment", "new_lexicon_in_sentiment",
                          "sentiment", "twitter_message", "word", "new_lexicon", "emoticon", "emoji"]
                for table in tables:
                    _execute_statement(cursor, f"DROP TABLE IF EXISTS `{table}`")
                connection.commit()

                _execute_statement(cursor, "CREATE TABLE `sentiment` ("
                                           "`id` int NOT NULL AUTO_INCREMENT,"
                                           "`type` varchar(32) NOT NULL UNIQUE,"
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
                                           "`token` varchar(140) NOT NULL UNIQUE,"
                                           "`slang` BOOLEAN NOT NULL DEFAULT FALSE,"
                                           "`meaning` varchar(1024) "
                                           "CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL DEFAULT '',"
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
                                           "PRIMARY KEY(`sentiment_id`,`tweet_id`),"
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

                sql_word = "INSERT INTO `word` (`word`) VALUES (%s) "
                sql_belongs_to = "INSERT INTO `belongs_to`(`sentiment_id`, `word_id`) VALUES (%s, %s)"
                sql_belongs_to_params = []

                first_insertion = True
                for word in sentiments_words:
                    _execute_statement(cursor, sql_word, [word])
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
                        words[t[{field}]] = t["id"]

                    sql_update_words = "UPDATE `{table}` SET `meaning` = %s, `slang` = %s WHERE `id` = %s"
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

                    _executemany_statements(cursor, sql_update_words, sql_params)
                    connection.commit()

    def __insert_tweets_sentiments(self, sentiments_tweets):
        """
        Inserts records into 'twitter_message' table and relations with 'sentiment' table
        @param sentiments_tweets: set with sentiments as keys and list of tweets as values
        """
        for sentiment in sentiments_tweets:
            with self.__connect_db() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SET NAMES utf8mb4')
                    cursor.execute("SET CHARACTER SET utf8mb4")
                    cursor.execute("SET character_set_connection=utf8mb4")

                    sql_insert_tweet = "INSERT INTO `twitter_message`(`tweet_content`) VALUES (%s) "
                    sql_assign_sentiment = "INSERT INTO `labelled`(`sentiment_id`, `tweet_id`) VALUES (%s, %s)"
                    sql_assign_sentiment_params = []
                    first_sentiment_tweet = True

                    id_sentiment = _execute_statement(cursor, f"SELECT `id` FROM `sentiment` "
                                                              f"WHERE `type` = \'{sentiment}\'").fetchone()["id"]
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
                for t in cursor.fetchall():
                    counts[t["type"]] = t["count"]
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
        # TODO: what if the word belongs to no sentiment?
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

    def get_tweets(self, sentiment: str):
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
                sql = "SELECT `id`, `tweet_content` " \
                      "FROM `twitter_message` LEFT JOIN `labelled` on `id` = `tweet_id` " \
                      "WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment])
                for t in cursor.fetchall():
                    tweets[t["id"]] = t["tweet_content"]
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
                sql = f"SELECT `id`, `{field}` " \
                      f"FROM `{token_type}` "
                cursor = _execute_statement(cursor, sql)
                for t in cursor.fetchall():
                    tokens[t[f"{token_type}"]] = t["id"]
        return tokens

    def add_tweets_tokens(self, tweets_tokens: {}):
        """
        Inserts the connection between tweets and tokens
        @param tweets_tokens: {tweet_id: [tokens]...}
        """
        # TODO: manage new tokens
        list_word = self.get_tokens("word")
        list_new_lexicon = self.get_tokens("new_lexicon")
        list_emoji = self.get_tokens("emoji")
        list_emoticon = self.get_tokens("emoticon")

        # data_token = {tweet_id: {token_id: count,...},...}
        data_word = {}
        data_new = {}
        data_emoji = {}
        data_emoticon = {}

        for tweet_id in tweets_tokens:
            for token in tweets_tokens[tweet_id]:
                token = token.lower()
                if token in list_word:
                    if tweet_id not in data_word or list_word[token] not in data_word[tweet_id]:
                        if tweet_id not in data_word:
                            data_word[tweet_id] = {}
                        data_word[tweet_id][list_word[token]] = 1
                    else:
                        data_word[tweet_id][list_word[token]] += 1

                elif token in list_emoji:
                    if tweet_id not in data_emoji or list_emoji[token] not in data_emoji[tweet_id]:
                        if tweet_id not in data_emoji:
                            data_emoji[tweet_id] = {}
                        data_emoji[tweet_id][list_emoji[token]] = 1
                    else:
                        data_emoji[tweet_id][list_emoji[token]] += 1

                elif token in list_emoticon:
                    if tweet_id not in data_emoticon or list_emoticon[token] not in data_emoticon[tweet_id]:
                        if tweet_id not in data_emoticon:
                            data_emoticon[tweet_id] = {}
                        data_emoticon[tweet_id][list_emoticon[token]] = 1
                    else:
                        data_emoticon[tweet_id][list_emoticon[token]] += 1

                else:
                    # TODO: wait the natural func
                    if token not in list_new_lexicon:
                        with self.__connect_db() as connection:
                            with connection.cursor() as cursor:
                                sql = "INSERT INTO `new_lexicon`(`token`) VALUES (%s)"
                                _execute_statement(cursor, sql, [token])
                                list_new_lexicon[token] = cursor.lastrowid
                    if tweet_id not in data_new or list_new_lexicon[token] not in data_new[tweet_id]:
                        if tweet_id not in data_new:
                            data_new[tweet_id] = {}
                        data_new[tweet_id][list_new_lexicon[token]] = 1
                    else:
                        data_new[tweet_id][list_new_lexicon[token]] += 1

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
        for token_type in data_to_insert:
            with self.__connect_db() as connection:
                with connection.cursor() as cursor:
                    if len(data_to_insert[token_type]) > 0:
                        for tweet_id in data_to_insert[token_type]:
                            sql = f"INSERT INTO `{token_type}_in_tweet` " \
                                  f"(`tweet_id`, `token_id`, `count`) VALUES (%s, %s, %s)"
                            sql_params = []
                            first_insert = True
                            for token_id in data_to_insert[token_type][tweet_id]:

                                if not first_insert:
                                    sql += ", (%s, %s, %s)"
                                else:
                                    first_insert = False
                                sql_params += [tweet_id, token_id, data_to_insert[token_type][tweet_id][token_id]]
                                # assert sql.count("%s") == len(sql_params)
                            _execute_statement(cursor, sql, sql_params)
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
                """ TODO: """
                sql = f"SELECT `{token_type}`, `{param}` " \
                      f"FROM `{token_type}` LEFT JOIN `{token_type}_in_sentiment` on `id` = `token_id` " \
                      f"WHERE `sentiment_id` = (SELECT `id` FROM `sentiment` WHERE `type` = %s)"
                cursor = _execute_statement(cursor, sql, [sentiment])
                for t in cursor.fetchall():
                    data[t[f"{token_type}"]] = t[f"{param}"]
        return data

    def build_sentiments(self, sentiments, word_datasets, emoji_datasets, emoticon_datasets):
        """
        Puts in the db the different datasets built in natural
        @param sentiments: array of sentiments
        @param word_datasets: a list of dicts for every word_frequency sentiment
        @param emoji_datasets: a list of dicts for every emoji_frequency sentiment
        @param emoticon_datasets: a list of dicts for every emoticon_frequency sentiment
        """
        list_word = self.get_tokens("word")
        list_emoji = self.get_tokens("emoji")
        list_emoticon = self.get_tokens("emoticon")
        sentiments_count = {sentiment: self.__get_word_numbers(sentiment) for sentiment in sentiments}

        for index, sentiment in enumerate(sentiments):
            with self.__connect_db() as connection:
                with connection.cursor() as cursor:
                    cursor.execute('SET NAMES utf8mb4')
                    cursor.execute("SET CHARACTER SET utf8mb4")
                    cursor.execute("SET character_set_connection=utf8mb4")

                    id_sentiment = _execute_statement(cursor, f"SELECT `id` FROM `sentiment` "
                                                              f"WHERE `type` = \'{sentiment}\'").fetchone()["id"]
                    assert id_sentiment >= 1

                    sql_intert = "INSERT INTO `word_in_sentiment` (`count`, `sentiment_id`, `token_id`, `freq_perc`) " \
                                 "VALUES (%s, %s, %s, %s)"
                    sql_params = []
                    for word in word_datasets[index]:
                        if word in list_word:
                            sql_params += [(word_datasets[index][word],
                                            id_sentiment,
                                            list_word[word],
                                            self.__calculate_popularity(word_datasets[index][word],
                                                                        sentiments_count[sentiment]))]
                    if sql_params is not []:
                        _executemany_statements(cursor, sql_intert, sql_params)
                        connection.commit()

                    sql_intert = "INSERT INTO `emoji_in_sentiment` (`count`, `sentiment_id`, `token_id`) " \
                                 "VALUES (%s, %s, %s)"
                    sql_params = []
                    for emoji in emoji_datasets[index]:
                        if emoji in list_emoji:
                            sql_params += [(word_datasets[index][word], id_sentiment, list_emoji[emoji])]
                    if sql_params is not []:
                        _executemany_statements(cursor, sql_intert, sql_params)
                        connection.commit()

                    sql_intert = "INSERT INTO `emoticon_in_sentiment` (`count`, `sentiment_id`, `token_id`) " \
                                 "VALUES (%s, %s, %s)"
                    sql_params = []
                    for emoticon in emoticon_datasets[index]:
                        if emoticon in list_emoticon:
                            sql_params += [(word_datasets[index][word], id_sentiment, list_emoticon[emoticon])]
                    if sql_params is not []:
                        _executemany_statements(cursor, sql_intert, sql_params)
                        connection.commit()

        # self.__insert_words_freq(sentiments, word_datasets)

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

    def get_sentiments_popularity(self) -> dict:
        """
        Gets the usage percentage of lexical words in tweets
        @return: a dict of all the percentages for every sentiment
        """
        freq = {}
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                sql = "SELECT `id`, `type`, SUM(`freq_perc`) AS tot " \
                      "FROM `word_in_sentiment` RIGHT JOIN `sentiment` ON `id` = `sentiment_id` " \
                      "GROUP BY `id`"
                cursor = _execute_statement(cursor, sql, [])
                for t in cursor.fetchall():
                    freq[t["type"]] = t["tot"]
        return freq


if __name__ == '__main__':
    """ 
    Tests
    """
    db = DaoMySQLDB()
    print(db.get_result("hello"))
