#!/usr/bin/python3
from collections import Counter
from pprint import pprint
from timeit import default_timer as timer

from emoji import UNICODE_EMOJI_ENGLISH
from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

from dao.dao import Dao
from file_manager import read_file
from lexical_glob import get_lexical_filenames, get_lexical_Nlines
from set_classification import negemoticons, posemoticons, twitter_stopwords
from src.datasets_manager import get_sentiment_words
from src.slang import create_definitions, preparse_standard_toml_files_sentiment, preparse_slang_toml_files_sentiment

tokenizer = TweetTokenizer()


def clean_emoticons(phrase: str):
    """
    Given a phrase, it remove every emoticon in it.
    @param phrase: sentence to clear from emoticons
    @return: the phrase free of emoticons and the list of emoticon
    """
    emoticon_list = [emoticon for emoticon in set(posemoticons + negemoticons) if emoticon in phrase]
    for emote in emoticon_list:
        phrase = phrase.replace(emote, '')
    return phrase, emoticon_list


def process_phrase(phrase: str, stopword_list: set):
    """
    Given a phrase and the stopwords list, it tokenizes the phrase removing the stopwords.
    Plus, we remove useless words (with "_" and "USERNAME" in it)
    @param phrase: sentence to be tokenized
    @param  stopword_list: the stopword list
    @return: the tokenized phrase
    """
    clean_phrase, emote_list = clean_emoticons(phrase)
    tokenized = tokenizer.tokenize(clean_phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list
                    and "_" not in word
                    and "USERNAME" not in word] + emote_list
    return final_phrase


def create_stopword_list() -> set:
    """
    Builds the stopword set.
    @return: the stopword set
    """
    default = stopwords.words('english')
    personal = twitter_stopwords
    final = default + personal
    return set(final)


def count_words(wordlist):
    """
    Counts the words in the tokenized phrases list.
    @param wordlist: list of tokenized phrases
    @return: words Counter, the # of times the words occur
    """
    final_list = []
    for tokenized_phrase in wordlist:
        for word in tokenized_phrase:
            if "." not in word and "$" not in word:
                final_list.append(word)
    return Counter(x for x in final_list)


def count_hashtags(count_tuples):
    """
    Returns a list of tuples ('#hashtagword', count) ordered from the most used
    to the least. Items that have only one usage are discarded.
    @param count_tuples:
    @return: list of Count tuples
    """
    return [item for item in count_tuples.most_common() if item[0].startswith('#')]


def count_emojis(count_tuples):
    """
    Returns a list of tuples ('emoji', count) ordered from the most used
    to the least. Items that have only one usage are discarded.
    @param count_tuples:
    @return: list of Count tuples
    """
    return [item for item in count_tuples.most_common() if item[0] in UNICODE_EMOJI_ENGLISH], \
           [item for item in count_tuples.most_common() if item[0] in posemoticons or item[0] in negemoticons]


def exclude_hastags_emojis(count_tuples):
    """
    Returns a list of tuples ('token', count) free of hashtags and emojis and
    ordered from the most used to the least. Items that have only one usage are discarded.
    @param count_tuples:
    @return: list of Count tuples
    """
    return [item for item in count_tuples.most_common() if not item[0].startswith('#')
            and not item[0] in UNICODE_EMOJI_ENGLISH and not item[0] in posemoticons and not item[0] in negemoticons]


def process_dataset(dao, tweets: dict, sentiment: str):
    """
    Given a datasets, it processes all its phrases line-by-line and
    extracts the wordlist and the hashtag list tuple made like
    ('word or hashtag', count) and returns the two different lists
    @return: list of Count tuples without hashtags and emojis,
    list of Count tuples of most used hashtags,
    list of Count tuples of emojis
    """
    print(f"Started NLP for {sentiment}")
    start = timer()
    wordlist = []
    stopset = create_stopword_list()
    tweets_tokens = {}
    if '_id' in tweets:
        tweets.pop('_id')
    for tweet_id, phrase in tweets.items():
        processed_phrase = process_phrase(phrase, stopset)
        wordlist.append(processed_phrase)
        tweets_tokens[tweet_id] = processed_phrase  # used in relational db
    dao.add_tweets_tokens(tweets_tokens)  # used in relational db
    count_tuples = count_words(wordlist)
    most_used_hashtags = count_hashtags(count_tuples)
    emojis, emoticons = count_emojis(count_tuples)
    count_tuples = exclude_hastags_emojis(count_tuples)
    end = timer()
    print(f"Done processing in {end - start} seconds")
    return dict(count_tuples), dict(most_used_hashtags), dict(emojis), dict(emoticons)


def convert_to_set(dataset):
    """
    Converts the dataset to a set.
    @param dataset: a Count list
    @return: a dataset set from the list
    """
    return set([word[0] for word in dataset])


def check_shared_words(word_datasets):
    """
    Given a datasets of phrases, it finds the words labelled as a Plutchik emotion
    @param word_datasets: datasets of phrases
    @return: dict of list af words foreach Plutchik emotion
    """
    files = get_lexical_filenames()
    shared_words = {"0": [], "1": [], "2": [], "3": [], "4": [], "5": [], "6": [], "7": []}
    for index, file in enumerate(files):
        twitter_words = word_datasets[index]
        for dataset in file:
            single = read_file(dataset)
            for word in single.splitlines():
                if word in twitter_words:
                    shared_words[f"{index}"].append(word)
    return shared_words


def calc_perc_sharedwords(shared_words, word_datasets):
    """
    Return a set of tuples (perc_presence_lex_res, perc_presence_twitter)
    @param shared_words: dict of list af words foreach Plutchik emotion
    @param word_datasets: datasets of phrases
    @return: set of tuples (perc_presence_lex_res, perc_presence_twitter)
    """
    linelist = get_lexical_Nlines()
    returndict = {}
    index = 0
    for wordlist in shared_words.values():
        n_shared_words = len(wordlist)
        n_twitter_words = len(word_datasets[index])
        print(f"Dataset: {index}, #Shared: {n_shared_words}, #Twitter: {n_twitter_words}, #Lex: {linelist[index]}")
        perc_presence_lex_res = n_shared_words / linelist[index]
        perc_presence_twitter = n_shared_words / n_twitter_words
        returndict[f"{index}"] = (perc_presence_lex_res, perc_presence_twitter)
        index += 1
    return returndict


def create_word_final_result(dao):
    """
    Creates the final result dict
    @param dao: the dao
    @return:  the final result dict
    """
    start = timer()
    word_datasets = []
    sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
    for sentiment in sentiments:
        word_datasets.append(dao.get_counts(sentiment, '_words'))

    result_list = []
    input_set = []
    for dataset in word_datasets:
        input_set += dataset
    input_set = set(input_set)

    if "_id" in input_set:
        input_set.remove("_id")
    for value in input_set:
        print(f"Creating result for word: {value}")
        count = dao.get_count(value)
        result_list.append(
            {
                "word": value,
                "count": count,
                "definition": dao.get_definition(value),
                "popularity": dao.get_popularity(value)
            }
        )
    dao.push_results(result_list)
    dao.create_index('word', 'results')  # Index on the attribute "word" in the table results
    end = timer()
    print(f"Done creating the final results in {end - start} seconds")


def dump_definitions_mongodb():
    """
    Dumps all the definitions on MongoDB. This function was made because we treat
    definitions in mongodb as a unique document, instead of an attribute of a
    pre-existent word in MySQL
    """
    dao = Dao(False)
    if dao.is_mongodb():
        sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
        for sentiment in sentiments:
            standard_toml_files = preparse_standard_toml_files_sentiment(sentiment)
            slang_toml_files = preparse_slang_toml_files_sentiment(sentiment)

            dao.dump_definitions(standard_toml_files, f"standard_definitions_{sentiment}")
            dao.dump_definitions(slang_toml_files, f"slang_definitions_{sentiment}")


def create_new_lexicon(word_datasets):
    """
    Creates a new wordlist of words that are not present
    in the pre-existent wordlist given by the project
    @param word_datasets: the list of dict of words processed by tweets
    @return: the new wordlist (see the definition)
    """
    new_wordlist = []
    existent_words = get_sentiment_words()

    for dataset in word_datasets:
        for word in dataset.keys():
            if word not in existent_words:
                new_wordlist.append(word)
    return new_wordlist


def quickstart(dao: Dao):
    """
    Quick start of the dataset sentiment analysis.
    """
    download('punkt')  # Scarica la punteggiatura dal server di nltk (one-time)
    download('stopwords')  # Scarica l'elenco di stopwords dal server di nltk (one-time)

    anger_words, anger_hashtags, anger_emojis, anger_emoticons = \
        process_dataset(dao, dao.get_tweets('anger'), 'anger_tweets')
    anticipation_words, anticipation_hashtags, anticipation_emojis, anticipation_emoticons = \
        process_dataset(dao, dao.get_tweets('anticipation'), 'anticipation_tweets')
    disgust_words, disgust_hashtags, disgust_emojis, disgust_emoticons = \
        process_dataset(dao, dao.get_tweets('disgust'), 'disgust_tweets')
    fear_words, fear_hashtags, fear_emojis, fear_emoticons = \
        process_dataset(dao, dao.get_tweets('fear'), 'fear_tweets')
    joy_words, joy_hashtags, joy_emojis, joy_emoticons = \
        process_dataset(dao, dao.get_tweets('joy'), 'joy_tweets')
    sadness_words, sadness_hashtags, sadness_emojis, sadness_emoticons = \
        process_dataset(dao, dao.get_tweets('sadness'), 'sadness_tweets')
    surprise_words, surprise_hashtags, surprise_emojis, surprise_emoticons = \
        process_dataset(dao, dao.get_tweets('surprise'), 'surprise_tweets')
    trust_words, trust_hashtags, trust_emojis, trust_emoticons = \
        process_dataset(dao, dao.get_tweets('trust'), 'trust_tweets')

    emoji_datasets = [anger_emojis, anticipation_emojis, disgust_emojis, fear_emojis, joy_emojis, sadness_emojis,
                      surprise_emojis, trust_emojis]
    word_datasets = [anger_words, anticipation_words, disgust_words, fear_words, joy_words, sadness_words,
                     surprise_words, trust_words]
    emoticons_datasets = [anger_emoticons, anticipation_emoticons, disgust_emoticons, fear_emoticons, joy_emoticons,
                          sadness_emoticons, surprise_emoticons, trust_emoticons]
    hashtag_datasets = [anger_hashtags, anticipation_hashtags, disgust_hashtags, fear_hashtags, joy_hashtags,
                        sadness_hashtags, surprise_hashtags, trust_hashtags]

    dao.build_sentiments(word_datasets, emoji_datasets, emoticons_datasets, hashtag_datasets)
    dao.dump_new_lexicon(create_new_lexicon(word_datasets))

    if input("Do you want to create the definitions of the words? (this can take up to 2 hours) [y/N] ").lower() == "y":
        create_definitions(word_datasets, dao)  # toml files
        if dao.is_mongodb():
            dump_definitions_mongodb()
        else:
            dao.dump_definitions()
        if dao.is_mongodb():
            if input("Do you want to create results for the words? [y/N] ").lower() == "y":
                create_word_final_result(dao)

    shared_words = check_shared_words(word_datasets)
    perc_calc = calc_perc_sharedwords(shared_words, word_datasets)
    pprint(perc_calc)
