#!/usr/bin/python3
import pymysql
from toml import load
from src.Natural import process_phrase, create_stopword_list
from src.lexical_glob import get_sentiment_words, get_sentiment_emojis, get_sentiment_emoticons


class Dao:
    def __init__(self):
        auth = load("../auth/auth.toml")
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

    def build_db(self):
        """
        Builds all tables of the relational db
        """
        print("Building DB")
        self.__drop_and_create_tables()
        print("Adding sentiments")
        self.__insert_sentiments()
        print("Adding words")
        self.__insert_words(get_sentiment_words())
        print("Adding emoticons")
        self.__insert_emoticons(get_sentiment_emojis())
        print("Adding emojis")
        self.__insert_emojis(get_sentiment_emoticons())

        # insert tweets
        # self.__insert_tweets(get_sentiment_tweets())

    def __drop_and_create_tables(self):
        """
        Drops tables if exist and creates them.
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS `labelled`")
                cursor.execute("DROP TABLE IF EXISTS `belongs_to`")
                cursor.execute("DROP TABLE IF EXISTS `in_tweet`")

                cursor.execute("DROP TABLE IF EXISTS `sentiment`")
                cursor.execute("CREATE TABLE `sentiment` (`id` int NOT NULL,`type` varchar(32) NOT NULL)")
                cursor.execute("ALTER TABLE `sentiment` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `sentiment` MODIFY `id` int NOT NULL AUTO_INCREMENT")
                connection.commit()

                cursor.execute("DROP TABLE IF EXISTS `twitter_message`")
                cursor.execute("CREATE TABLE `twitter_message` ("
                               "`id` int NOT NULL,`tweet_content` varchar(280) NOT NULL)")
                cursor.execute("ALTER TABLE `twitter_message` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `twitter_message` MODIFY `id` int NOT NULL AUTO_INCREMENT")
                connection.commit()

                cursor.execute("DROP TABLE IF EXISTS `word`")
                cursor.execute("CREATE TABLE `word` (`id` int NOT NULL,`word` varchar(64) NOT NULL)")
                cursor.execute("ALTER TABLE `word` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `word` MODIFY `id` int NOT NULL AUTO_INCREMENT")
                connection.commit()

                cursor.execute("DROP TABLE IF EXISTS `emoticon`")
                cursor.execute("CREATE TABLE `emoticon` ("
                               "`id` int NOT NULL,"
                               "`emoticon` varchar(32) CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL, "
                               "`polarity` set('positive','negative','neutral','other') NOT NULL)")
                cursor.execute("ALTER TABLE `emoticon` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `emoticon` MODIFY `id` int NOT NULL AUTO_INCREMENT")
                connection.commit()

                cursor.execute("DROP TABLE IF EXISTS `emoji`")
                cursor.execute("CREATE TABLE `emoji` ("
                               "`id` int NOT NULL,"
                               "`emoji` varchar(32) CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL, "
                               "`polarity` set('positive','negative','neutral','other') NOT NULL)")
                cursor.execute("ALTER TABLE `emoji` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `emoji` MODIFY `id` int NOT NULL AUTO_INCREMENT")
                connection.commit()

                cursor.execute("CREATE TABLE `labelled` ("
                               "`sentiment_id` int NOT NULL, "
                               "`tweet_id` int NOT NULL)")
                cursor.execute("ALTER TABLE `labelled`"
                               "ADD PRIMARY KEY (`sentiment_id`,`tweet_id`), "
                               "ADD KEY `tweet_id` (`tweet_id`);")
                cursor.execute("ALTER TABLE `labelled` "
                               "ADD CONSTRAINT `labelled_sentiment_fk` "
                               "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`), "
                               "ADD CONSTRAINT `tweet_labelled_fk` "
                               "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`);")
                connection.commit()

                cursor.execute("CREATE TABLE `belongs_to` ("
                               "`sentiment_id` int NOT NULL,"
                               "`word_id` int NOT NULL)")
                cursor.execute("ALTER TABLE `belongs_to` "
                               "ADD KEY `word_id` (`word_id`), "
                               "ADD KEY `sentiment_id` (`sentiment_id`);")
                cursor.execute("ALTER TABLE `belongs_to` "
                               "ADD CONSTRAINT `word_belongs_to_fk` "
                               "FOREIGN KEY (`word_id`) REFERENCES `word` (`id`), "
                               "ADD CONSTRAINT `belongs_to_sentiment_fk` "
                               "FOREIGN KEY (`sentiment_id`) REFERENCES `sentiment` (`id`);")
                connection.commit()

                cursor.execute("CREATE TABLE `in_tweet` ("
                               "`tweet_id` int NOT NULL, "
                               "`token_id` int NOT NULL,"
                               "`type` SET('WORD','EMOJI','EMOTICON') NOT NULL)")
                cursor.execute("ALTER TABLE `in_tweet` "
                               "ADD PRIMARY KEY (`tweet_id`,`token_id`);")
                cursor.execute("ALTER TABLE `in_tweet` "
                               "ADD CONSTRAINT `in_tweet_fk` "
                               "FOREIGN KEY (`tweet_id`) REFERENCES `twitter_message` (`id`);")
                connection.commit()

    def __insert_sentiments(self):
        """
        Builds sentiment table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
                sql = "INSERT INTO `sentiment` (`type`) VALUES (%s)"
                for s in sentiments:
                    cursor.execute(sql, s)
                connection.commit()

    def __insert_words(self, word_sentiments: {}):
        """
        Builds word table
        """
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

                cursor.execute(sql_word, sql_word_params)
                connection.commit()
                cursor.execute(sql_belongs_to, sql_belongs_to_params)
                connection.commit()

                print(f"{len(word_sentiments)} words added")
                print(f"{sum([len(word_sentiments[k]) for k in word_sentiments])} relations added")

    def __insert_emoticons(self, polarity_emoticons: {}):
        """
        Builds emoticon table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `emoticon` (`emoticon`, polarity) VALUES (%s, %s)"
                for index in polarity_emoticons:
                    for emoticon in polarity_emoticons[index]:
                        cursor.execute(sql, (emoticon, index))
                connection.commit()

    def __insert_emojis(self, polarity_emojis: {}):
        """
        Builds emoji table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                sql = "INSERT INTO `emoji` (`emoji`, polarity) VALUES (%s, %s)"
                for index in polarity_emojis:
                    for emoji in polarity_emojis[index]:
                        cursor.execute(sql, (emoji, index))
                connection.commit()

    # TODO
    def __insert_tweets(self, tweets_content: [str]):
        """
        Builds twitter_message table
        @param tweets_content: list of tweets' content
        """

    # TODO add __single_query(statement:str)
    # TODO add __single_exec(statement:str)


if __name__ == '__main__':
    """ DAO execs. """
    Dao().build_db()
