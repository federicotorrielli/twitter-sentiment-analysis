#!/usr/bin/python3
import pprint

import pymongo

from src.file_manager import read_file


class DaoMongoDB:
    def __init__(self):
        """ Mongo DB """
        self.client = pymongo.MongoClient(
            "mongodb+srv://database:ivanfederico@cluster0.9hwxj.mongodb.net/twitter"
            "?retryWrites=true&w=majority")
        self.database = self.client.database

    def build_db(self, sentiments, words, emoticons, emojis, tweets):
        """
        Builds all tables of mongodb
        """
        # TODO: build the db from the ground up >
        #  https://docs.atlas.mongodb.com/tutorial/insert-data-into-your-cluster/ and
        #  https://pymongo.readthedocs.io/en/stable/tutorial.html

        # First we drop everything, just to make sure
        self.database.command('dropDatabase')

        for index, sentiment in enumerate(sentiments):
            tweet_document = self.database[f'{sentiment}_tweets']
            self.__insert_tweets(tweet_document, tweets[index], sentiment)
            word_document = self.database[f'{sentiment}_words']
            word_document.insert(words)
            emoji_document = self.database[f'{sentiment}_emoji']
            emoji_document.insert(emojis)
            emoticon_document = self.database[f'{sentiment}_emoticons']
            emoticon_document.insert(emoticons)

    def build_sentiments(self, sentiments, word_datasets, emoji_datasets, emoticon_datasets):
        # We insert everything as specified
        for index, sentiment in enumerate(sentiments):
            word_table = self.database[f'{sentiment}_words_frequency']
            emoji_table = self.database[f'{sentiment}_emoji_frequency']
            emoticon_table = self.database[f'{sentiment}_emoticon_frequency']
            word_table.insert(word_datasets[index], check_keys=False)
            emoji_table.insert(emoji_datasets[index], check_keys=False)
            emoticon_table.insert(emoticon_datasets[index], check_keys=False)

    def _easy_statement_exec(self, statement: str, params: [str]):
        """
        Given a statement and the parameters eventually needed, it execs the statement
        @param statement:
        @param params: parameters eventually needed inside the statement
        @return: cursor
        """
        # TODO:

    def __get_collection(self, collection_name: str):
        return self.database[collection_name]

    def find_count(self, key, collection_name):
        """
        Given the key, it returns the count of it
        @param collection_name: the name of the mongo document (like anger_word)
        @param key: the word or emoji you are trying to find
        @return: the number of times that word appeared, the id of the object
        """
        collection = self.__get_collection(collection_name)
        result = collection.find({}, {key: 1})[0]
        return result[key], result['_id']

    def get_document(self, collection_name):
        """
        Given a collection name it returns the dict containing the document
        @param collection_name: name of the mongo collection
        @return: dict of the document
        """
        return self.__get_collection(collection_name).find()[0]

    def test_db(self):
        """
        Runs a test of the cluster connection to the endpoint
        @return: the test dict
        """
        return self.client.test

    def __insert_tweets(self, tweet_document, file_path, sentiment):
        file = read_file(file_path)
        tweet_dict = {}
        for key, line in enumerate(file.splitlines()):
            tweet_dict[f'{sentiment}_{key}'] = line
        tweet_document.insert(tweet_dict)

    def dump_definitions(self, definitions: dict, name: str):
        definition_document = self.database[f'{name}']
        definition_document.insert(definitions)

    def get_definition(self, word: str, sentiment: str = "") -> str:
        """
        Gets the definition of the given word
        @param word:
        @param sentiment: optionally you can input a specific sentiment
        @return: the definition
        """
        if sentiment == "":
            sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        else:
            sentiments = [sentiment]
        # If we don't define a sentiment, we cycle through them all
        for s in sentiments:
            definition_document = self.database[f'standard_definitions_{s}']
            slang_document = self.database[f'slang_definitions_{s}']
            result1 = definition_document.find({}, {word: 1})[0]
            result2 = slang_document.find({}, {word: 1})[0]
            if word in result1:
                return result1[word]
            elif word in result2:
                return result2[word]
            else:
                pass
        return "NOTHING FOUND"


if __name__ == '__main__':
    dao = DaoMongoDB()
    print(dao.get_definition("classy"))
