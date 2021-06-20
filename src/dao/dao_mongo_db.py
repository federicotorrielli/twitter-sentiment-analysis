#!/usr/bin/python3
import pymongo
from src.file_manager import read_file


class DaoMongoDB:
    def __init__(self):
        """ Mongo DB """
        self.client = pymongo.MongoClient(
            "mongodb+srv://database:ivanfederico@cluster0.9hwxj.mongodb.net/twitter"
            "?retryWrites=true&w=majority")
        self.database = self.client.database
        self.word_numbers = {"anger": 0, "anticipation": 0, "disgust": 0, "fear": 0, "joy": 0, "sadness": 0,
                             "surprise": 0, "trust": 0}
        self.std_definitions = {"anger": {}, "anticipation": {}, "disgust": {}, "fear": {}, "joy": {}, "sadness": {},
                                "surprise": {}, "trust": {}}
        self.slang_definitions = {"anger": {}, "anticipation": {}, "disgust": {}, "fear": {}, "joy": {}, "sadness": {},
                                  "surprise": {}, "trust": {}}
        self.frequencies = {"anger": {}, "anticipation": {}, "disgust": {}, "fear": {}, "joy": {}, "sadness": {},
                            "surprise": {}, "trust": {}}

    def build_db(self, sentiments, words, emoticons, emojis, tweets):
        """
        Builds all tables of mongodb
        """
        # TODO: build the db from the ground up >
        #  https://docs.atlas.mongodb.com/tutorial/insert-data-into-your-cluster/ and
        #  https://pymongo.readthedocs.io/en/stable/tutorial.html

        # First we drop everything, just to make sure
        print("Dropping the previous database...")
        self.database.command('dropDatabase')

        print("Inserting the tweets on the database...")
        for index, sentiment in enumerate(sentiments):
            tweet_document = self.database[f'{sentiment}_tweets']
            self.__insert_tweets(tweet_document, tweets[index], sentiment)
            word_document = self.database[f'{sentiment}_words']
            word_document.insert(words)
            emoji_document = self.database[f'{sentiment}_emoji']
            emoji_document.insert(emojis)
            emoticon_document = self.database[f'{sentiment}_emoticons']
            emoticon_document.insert(emoticons)

    def __get_word_numbers(self, sentiment):
        if self.word_numbers[sentiment] == 0:
            self.word_numbers[sentiment] = len(self.get_document(f"{sentiment}_words"))
        return self.word_numbers[sentiment]

    def build_sentiments(self, sentiments, word_datasets, emoji_datasets, emoticon_datasets):
        # We insert everything as specified
        for index, sentiment in enumerate(sentiments):
            word_table = self.database[f'{sentiment}_words_frequency']
            emoji_table = self.database[f'{sentiment}_emoji_frequency']
            emoticon_table = self.database[f'{sentiment}_emoticon_frequency']
            word_table.insert(word_datasets[index], check_keys=False)
            emoji_table.insert(emoji_datasets[index], check_keys=False)
            emoticon_table.insert(emoticon_datasets[index], check_keys=False)

    def __get_collection(self, collection_name: str):
        return self.database[collection_name]

    def get_count(self, word) -> dict:
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        final_list = {}
        for sentiment in sentiments:
            if len(self.frequencies[sentiment]) == 0:
                self.frequencies[sentiment] = self.get_document(f"{sentiment}_words_frequency")

            if word in self.frequencies[sentiment]:
                final_list[sentiment] = self.frequencies[sentiment][word]
        return final_list

    def get_document(self, collection_name):
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
        if sentiment == "":
            sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        else:
            sentiments = [sentiment]
        # If we don't define a sentiment, we cycle through them all
        for s in sentiments:
            if len(self.std_definitions[s]) == 0 or len(self.slang_definitions[s]) == 0:
                self.std_definitions[s] = self.get_document(f'standard_definitions_{s}')
                self.slang_definitions[s] = self.get_document(f'slang_definitions_{s}')

            if word in self.std_definitions[s]:
                return self.std_definitions[s][word]
            if word in self.slang_definitions[s]:
                return self.slang_definitions[s][word]
        return "NOTHING FOUND"

    def push_result(self, word: str, count: dict, definition: str, popularity: dict):
        if word != "_id" and "." not in word and "$" not in word:
            results = self.__get_collection("results")
            results.insert_one({
                "word": word,
                "count": count,
                "definition": definition,
                "popularity": popularity
            })

    def push_results(self, result_list):
        results = self.__get_collection("results")
        results.insert_many(result_list)

    def get_result(self, word: str) -> dict:
        results = self.__get_collection("results")
        word_result = results.find_one({"word": word})
        return word_result

    def get_popularity(self, word: str, count=None):
        if count is None:
            count = self.get_count(word)
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        final_list = {}
        for sentiment in sentiments:
            if sentiment in count:
                final_list[sentiment] = \
                    f"{count[sentiment] / self.__get_word_numbers(sentiment)}%"
        return final_list

    def create_index(self, index: str, table: str):
        t = self.__get_collection(table)
        t.create_index([(index, pymongo.ASCENDING)])
