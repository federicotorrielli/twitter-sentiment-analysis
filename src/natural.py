#!/usr/bin/python3
from collections import Counter
from glob import glob
from pprint import pprint

from emoji import UNICODE_EMOJI_ENGLISH
from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

from file_manager import read_file
from set_classification import negemoticons, posemoticons, twitter_stopwords
# from slang import create_definitions
from lexical_glob import get_lexical_filenames, get_lexical_Nlines

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
    @param phrase: sentence to be tokenized
    @param  stopword_list:
    @return: the tokenized phrase
    """
    clean_phrase, emote_list = clean_emoticons(phrase)
    tokenized = tokenizer.tokenize(clean_phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list and "_" not in word] + emote_list
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
    return [item for item in count_tuples.most_common() if item[0] in UNICODE_EMOJI_ENGLISH
            or item[0] in posemoticons or item[0] in negemoticons]


def exclude_hastags_emojis(count_tuples):
    """
    Returns a list of tuples ('token', count) free of hashtags and emojis and
    ordered from the most used to the least. Items that have only one usage are discarded.
    @param count_tuples:
    @return: list of Count tuples
    """
    return [item for item in count_tuples.most_common() if not item[0].startswith('#')
            and not item[0] in UNICODE_EMOJI_ENGLISH and not item[0] in posemoticons and not item[0] in negemoticons]


def process_dataset(file_path: str):
    """
    Given a datasets, it processes all its phrases line-by-line and
    extracts the wordlist and the hashtag list tuple made like
    ('word or hashtag', count) and returns the two different lists
    @param file_path:
    @return: list of Count tuples without hashtags and emojis,
    list of Count tuples of most used hashtags,
    list of Count tuples of emojis
    """
    wordlist = []
    file = read_file(file_path)
    stopset = create_stopword_list()
    for phrase in file.splitlines():
        processed_phrase = process_phrase(phrase, stopset)
        wordlist.append(processed_phrase)
    count_tuples = count_words(wordlist)
    most_used_hashtags = count_hashtags(count_tuples)
    emojis = count_emojis(count_tuples)
    count_tuples = exclude_hastags_emojis(count_tuples)
    return count_tuples, most_used_hashtags, emojis


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
    @return: a dict of a list af words foreach Plutchik emotion
    """
    # TODO needs explanation:
    #  why does it add to the dict only the words of the main phrase sentiment?
    files = get_lexical_filenames()
    shared_words = {"0": [], "1": [], "2": [], "3": [], "4": [], "5": [], "6": [], "7": []}
    for index, file in enumerate(files):
        twitter_words = convert_to_set(word_datasets[index])
        for dataset in file:
            single = read_file(dataset)
            for word in single.splitlines():
                if word in twitter_words:
                    shared_words[f"{index}"].append(word)
    return shared_words


def calc_perc_sharedwords(shared_words, word_datasets):
    """
    Return a dict of tuples (, ) # TODO
    @param shared_words:
    @param word_datasets:
    @return:
    """
    # TODO con che percentuale le parole si propongono in N frasi oppure in M parole?
    #  Non conta le percentuale di presenza di una parola nel dataset ma nel dizionario
    linelist = get_lexical_Nlines()
    returndict = {}
    index = 0
    for wordlist in shared_words.values():
        N_shared_words = len(wordlist)
        N_twitter_words = len(word_datasets[index])
        print(f"Dataset: {index}, #Shared: {N_shared_words}, #Twitter: {N_twitter_words}, #Lex: {linelist[index]}")
        perc_presence_lex_res = N_shared_words / linelist[index]
        perc_presence_twitter = N_shared_words / N_twitter_words
        returndict[f"{index}"] = (perc_presence_lex_res, perc_presence_twitter)
        index += 1
    return returndict


def quickstart():
    """
    Quick start of the dataset sentiment analysis.
    """
    anger_dataset = glob("../Resources/tweets/dataset_dt_anger_60k.txt")
    anticipation_dataset = glob("../Resources/tweets/dataset_dt_anticipation_60k.txt")
    disgust_dataset = glob("../Resources/tweets/dataset_dt_disgust_60k.txt")
    fear_dataset = glob("../Resources/tweets/dataset_dt_fear_60k.txt")
    joy_dataset = glob("../Resources/tweets/dataset_dt_joy_60k.txt")
    sadness_dataset = glob("../Resources/tweets/dataset_dt_sadness_60k.txt")
    surprise_dataset = glob("../Resources/tweets/dataset_dt_surprise_60k.txt")
    trust_dataset = glob("../Resources/tweets/dataset_dt_trust_60k.txt")

    anger_words, anger_hashtags, anger_emojis = process_dataset(anger_dataset[0])
    anticipation_words, anticipation_hashtags, anticipation_emojis = process_dataset(anticipation_dataset[0])
    disgust_words, disgust_hashtags, disgust_emojis = process_dataset(disgust_dataset[0])
    fear_words, fear_hashtags, fear_emojis = process_dataset(fear_dataset[0])
    joy_words, joy_hashtags, joy_emojis = process_dataset(joy_dataset[0])
    sadness_words, sadness_hashtags, sadness_emojis = process_dataset(sadness_dataset[0])
    surprise_words, surprise_hashtags, surprise_emojis = process_dataset(surprise_dataset[0])
    trust_words, trust_hashtags, trust_emojis = process_dataset(trust_dataset[0])
    word_datasets = [anger_words, anticipation_words, disgust_words, fear_words, joy_words, sadness_words,
                     surprise_words, trust_words]
    shared_words = check_shared_words(word_datasets)
    perc_calc = calc_perc_sharedwords(shared_words, word_datasets)
    pprint(perc_calc)
    # create_definitions(word_datasets)


if __name__ == '__main__':
    download('punkt')
    download('stopwords')
    quickstart()
