#!/usr/bin/python3
import pymysql
from toml import load
from set_classification import posemoticons, negemoticons, EmojiPos, EmojiNeg, OthersEmoji


class Dao:
    def __init__(self):
        auth = load("../auth/auth.toml")
        self.user = auth.get("user")
        self.pwd = auth.get("password")
        self.host = auth.get("host")
        self.db = auth.get("db")

    def __connect_db(self):
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
        Build all tables of the relational db
        """
        self.__insert_sentiments()
        # self.__insert_sentiments()
        # self.__insert_icons()

    def __insert_tweets(self):
        """
        Build twitter_message table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                cursor.execute("DROP TABLE IF EXISTS `twitter_message`")
                cursor.execute("CREATE TABLE `twitter_message` ("
                               "`id` int NOT NULL,`tweet_content` varchar(280) NOT NULL)")
                cursor.execute("ALTER TABLE `twitter_message` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `twitter_message` MODIFY `id` int NOT NULL AUTO_INCREMENT")

                # TODO get tweets tokenized from evilscript func
                tweets = []
                sql = "INSERT INTO `twitter_message` (`tweet_content`) VALUES (%s)"
                for t in tweets:
                    cursor.execute(sql, t)
                    # TODO add sentiments (?)
                connection.commit()

    def __insert_words(self):
        """
        Build word table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                cursor.execute("DROP TABLE IF EXISTS `word`")
                cursor.execute("CREATE TABLE `word` (`id` int NOT NULL,`word` varchar(64) NOT NULL)")
                cursor.execute("ALTER TABLE `word` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `word` MODIFY `id` int NOT NULL AUTO_INCREMENT")

                # TODO get words classified from evilscript func
                tweets = []
                sql = "INSERT INTO `word` (`word`) VALUES (%s)"
                for t in tweets:
                    cursor.execute(sql, t)
                    # TODO add sentiments (?)
                connection.commit()

    def __insert_sentiments(self):
        """
        Build sentiment table
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                cursor.execute("DROP TABLE IF EXISTS `sentiment`")
                cursor.execute("CREATE TABLE `sentiment` (`id` int NOT NULL,`type` varchar(32) NOT NULL)")
                cursor.execute("ALTER TABLE `sentiment` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `sentiment` MODIFY `id` int NOT NULL AUTO_INCREMENT")

                sentiments = ["anger", "anticipation", "joy", "trust", "fear", "surprise", "sadness", "disgust"]
                sql = "INSERT INTO `sentiment` (`type`) VALUES (%s)"
                for s in sentiments:
                    cursor.execute(sql, s)
                connection.commit()

    def __insert_icons(self):
        """
        Build emoji and emoticon tables
        """
        with self.__connect_db() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                cursor.execute("DROP TABLE IF EXISTS `emoticon`")
                cursor.execute("CREATE TABLE `emoticon` ("
                               "`id` int NOT NULL,"
                               "`emoticon` varchar(32) CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL, "
                               "`polarity` set('positive','negative','neutral','other') NOT NULL)")
                cursor.execute("ALTER TABLE `emoticon` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `emoticon` MODIFY `id` int NOT NULL AUTO_INCREMENT")

                sql = "INSERT INTO `emoticon` (`emoticon`, polarity) VALUES (%s, %s)"
                for emoticon in posemoticons:
                    cursor.execute(sql, (emoticon, 'positive'))
                for emoticon in negemoticons:
                    cursor.execute(sql, (emoticon, 'negative'))
                connection.commit()

            with connection.cursor() as cursor:
                cursor.execute('SET NAMES utf8mb4')
                cursor.execute("SET CHARACTER SET utf8mb4")
                cursor.execute("SET character_set_connection=utf8mb4")

                cursor.execute("DROP TABLE IF EXISTS `emoji`")
                cursor.execute("CREATE TABLE `emoji` ("
                               "`id` int NOT NULL,"
                               "`emoji` varchar(32) CHARACTER SET utf32 COLLATE utf32_general_ci NOT NULL, "
                               "`polarity` set('positive','negative','neutral','other') NOT NULL)")
                cursor.execute("ALTER TABLE `emoji` ADD PRIMARY KEY (`id`)")
                cursor.execute("ALTER TABLE `emoji` MODIFY `id` int NOT NULL AUTO_INCREMENT")

                sql = "INSERT INTO `emoji` (`emoji`, polarity) VALUES (%s, %s)"
                for emoji in EmojiPos:
                    cursor.execute(sql, (emoji, 'positive'))
                for emoji in EmojiNeg:
                    cursor.execute(sql, (emoji, 'negative'))
                for emoji in OthersEmoji:
                    cursor.execute(sql, (emoji, 'other'))
                connection.commit()


if __name__ == '__main__':
    """ DAO execs. """
    Dao().build_db()
