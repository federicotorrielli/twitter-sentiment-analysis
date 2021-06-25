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
        self.word_counts = {"anger": {}, "anticipation": {}, "disgust": {}, "fear": {}, "joy": {}, "sadness": {},
                            "surprise": {}, "trust": {}}

    def build_db(self, sentiments, words, emoticons, emojis, twitter_paths):
        """
        Builds all tables of mongodb
        @param sentiments: list of sentiments
        @param words: set with words as keys and list of sentiments ids as values
        @param emoticons: set with polarity as keys and list of emoticons as values
        @param emojis: set with polarity as keys and list of emoji as values
        @param twitter_paths: list of file paths that contains tweets
        """
        print("Dropping the previous database...")
        self.database.command('dropDatabase')

        print("Inserting the tweets on the database...")
        for index, sentiment in enumerate(sentiments):
            tweet_document = self.database[f'{sentiment}_tweets']
            self.__insert_tweets(tweet_document, twitter_paths[index], sentiment)
            word_document = self.database[f'{sentiment}_words']
            self.__insert_words(word_document, words, index)
            emoji_document = self.database[f'{sentiment}_emoji']
            emoji_document.insert(emojis)
            emoticon_document = self.database[f'{sentiment}_emoticons']
            emoticon_document.insert(emoticons)

    def __get_word_numbers(self, sentiment):
        if self.word_numbers[sentiment] == 0:
            self.word_numbers[sentiment] = len(self.get_document(f"{sentiment}_words"))
        return self.word_numbers[sentiment]

    def __get_collection_address(self, collection_name: str):
        return self.database[collection_name]

    def __insert_words(self, word_document, words, sentiment):
        words_to_add = {}
        for word, sentiments in words.items():
            if sentiment in sentiments:
                words_to_add[word] = sentiments
        word_document.insert(words_to_add)

    def __insert_tweets(self, tweet_document, file_path, sentiment):
        file = read_file(file_path)
        tweet_dict = {}
        for key, line in enumerate(file.splitlines()):
            tweet_dict[f'{sentiment}_{key}'] = line
        tweet_document.insert(tweet_dict)

    def build_sentiments(self, sentiments, word_datasets, emoji_datasets, emoticon_datasets, hashtag_datasets):
        to_add_words, to_add_emojis, to_add_emoticons, to_add_hashtags = {}, {}, {}, {}
        for index, sentiment in enumerate(sentiments):
            to_add_words[sentiment] = word_datasets[index]
            to_add_emojis[sentiment] = emoji_datasets[index]
            to_add_emoticons[sentiment] = emoticon_datasets[index]
            to_add_hashtags[sentiment] = hashtag_datasets[index]
        sentiments_word_table = self.database['count_words']
        sentiments_word_table.insert_one(to_add_words)
        sentiments_emoji_table = self.database['count_emoji']
        sentiments_emoji_table.insert_one(to_add_emojis)
        sentiments_emoticon_table = self.database['count_emoticon']
        sentiments_emoticon_table.insert_one(to_add_emoticons)
        sentiment_hashtag_table = self.database['count_hashtags']
        sentiment_hashtag_table.insert_one(to_add_hashtags)

    def create_index(self, index: str, table: str):
        t = self.__get_collection_address(table)
        t.create_index([(index, pymongo.ASCENDING)])

    def get_count(self, word) -> dict:
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        final_list = {}
        for sentiment in sentiments:
            if len(self.word_counts[sentiment]) == 0:
                sentiments_word_table = self.database['count_words']
                self.word_counts[sentiment] = sentiments_word_table.find_one({}, {sentiment})[sentiment]

            if word in self.word_counts[sentiment]:
                final_list[sentiment] = self.word_counts[sentiment][word]
        return final_list

    def get_document(self, collection_name):
        # NOTE: this gets only the first document in the collection_name collection
        return self.__get_collection_address(collection_name).find_one()

    def get_tweets(self, sentiment):
        sentiment += "_tweets"
        return self.get_document(sentiment)

    def get_counts(self, sentiment, token_type: str = ""):
        sentiments_table = self.database[f'count{token_type}']
        return sentiments_table.find_one({}, {sentiment})[sentiment]

    def get_collection(self, collection_name):
        return self.__get_collection_address(collection_name).find()

    def get_definition(self, word: str, sentiment: str = "") -> str:
        if sentiment == "":
            sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        else:
            sentiments = [sentiment]

        for s in sentiments:
            if len(self.std_definitions[s]) == 0 or len(self.slang_definitions[s]) == 0:
                self.std_definitions[s] = self.get_document(f'standard_definitions_{s}')
                self.slang_definitions[s] = self.get_document(f'slang_definitions_{s}')

            if word in self.std_definitions[s]:
                return self.std_definitions[s][word]
            if word in self.slang_definitions[s]:
                return self.slang_definitions[s][word]
        return "NOTHING FOUND"

    def get_result(self, word: str) -> dict:
        results = self.__get_collection_address("results")
        word_result = results.find_one({"word": word})
        return word_result

    def get_popularity(self, word: str, count=None):
        if count is None:
            count = self.get_count(word)
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        final_list = {}
        for sentiment in sentiments:
            if sentiment in count:
                final_list[sentiment] = round(count[sentiment] / self.__get_word_numbers(sentiment), 4)
        return final_list

    def dump_definitions(self, definitions: dict, name: str):
        definition_document = self.database[f'{name}']
        definition_document.insert(definitions)

    def push_result(self, word: str, count: dict, definition: str, popularity: dict):
        if word != "_id" and "." not in word and "$" not in word:
            results = self.__get_collection_address("results")
            results.insert_one({
                "word": word,
                "count": count,
                "definition": definition,
                "popularity": popularity
            })

    def push_results(self, result_list):
        results = self.__get_collection_address("results")
        results.insert_many(result_list)

    def get_sentiments_popularity(self) -> dict:
        """
        Gets the usage percentage of lexical words in tweets
        @return: a dict of all the percentages for every sentiment
        """
        results = self.__get_collection_address("results").find({})
        sentiments_popularity = {}
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        for sentiment in sentiments:
            sentiments_popularity[sentiment] = 0
        for res in results:
            for s in res["popularity"]:
                sentiments_popularity[s] += res["popularity"][s]
        return sentiments_popularity


if __name__ == '__main__':
    dao = DaoMongoDB()
    # pprint.pprint(dao.get_counts("anticipation", "_words")['anticipation'])
    dao.get_sentiments_popularity()