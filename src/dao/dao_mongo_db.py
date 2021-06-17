#!/usr/bin/python3
import pymongo


class DaoMongoDB:
    def __init__(self, word_datasets, emoji_datasets, emoticon_datasets):
        """ Mongo DB """
        self.client = pymongo.MongoClient(
            "mongodb+srv://database:ivanfederico@cluster0.9hwxj.mongodb.net/twitter"
            "?retryWrites=true&w=majority")
        self.database = self.client.database
        self.emoticon_datasets = emoticon_datasets
        self.emoji_datasets = emoji_datasets
        self.word_datasets = word_datasets

    def build_db(self, sentiments, words, emoticons, emojis, tweets):
        """
        Builds all tables of mongodb
        """
        # TODO: build the db from the ground up >
        #  https://docs.atlas.mongodb.com/tutorial/insert-data-into-your-cluster/ and
        #  https://pymongo.readthedocs.io/en/stable/tutorial.html

        # First we drop everything, just to make sure
        self.database.command('dropDatabase')

        # Then we insert everything as specified
        for index, sentiment in enumerate(sentiments):
            word_table = self.database[f'{sentiment}_word']
            emoji_table = self.database[f'{sentiment}_emoji']
            emoticon_table = self.database[f'{sentiment}_emoticon']
            word_table.insert(self.word_datasets[index], check_keys=False)
            emoji_table.insert(self.emoji_datasets[index], check_keys=False)
            emoticon_table.insert(self.emoticon_datasets[index], check_keys=False)

    def _easy_statement_exec(self, statement: str, params: [str]):
        """
        Given a statement and the parameters eventually needed, it execs the statement
        @param statement:
        @param params: parameters eventually needed inside the statement
        @return: cursor
        """
        # TODO:

    def test_db(self):
        """
        Runs a test of the cluster connection to the endpoint
        @return: the test dict
        """
        return self.client.test
