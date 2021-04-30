#!/usr/bin/python3
from glob import glob
from concurrent.futures import ThreadPoolExecutor

from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from collections import Counter

from FileManager import read_file
from set_classification import twitter_stopwords, posemoticons, negemoticons

tokenizer = TweetTokenizer()


def clean_emoticons(phrase: str):
    emoticon_list = [emoticon for emoticon in (posemoticons + negemoticons) if emoticon in phrase]
    for emote in emoticon_list:
        phrase = phrase.replace(emote, '')
    return phrase, emoticon_list


def process_phrase(phrase: str, stopword_list: set):
    clean_phrase, emote_list = clean_emoticons(phrase)
    tokenized = tokenizer.tokenize(clean_phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list and "_" not in word] + emote_list
    return final_phrase


def create_stopword_list() -> set:
    default = stopwords.words('english')
    personal = twitter_stopwords
    final = default + personal
    return set(final)


def count_words(wordlist):
    final_list = []
    for tokenzied_phrase in wordlist:
        for word in tokenzied_phrase:
            final_list.append(word)
    return Counter(x for x in final_list)


def count_hashtags(count_tuples):
    """
    Returns a list of tuples ('#hashtagword', count) ordered from the most used
    to the least. Items that have only one usage are discarded.
    :param count_tuples:
    :return: list of Count tuples
    """
    return [item for item in count_tuples.most_common() if item[-1] > 1 and item[0].startswith('#')]


def exclude_hastags(count_tuples):
    return [item for item in count_tuples.most_common() if item[-1] > 1 and not item[0].startswith('#')]


def process_dataset(file_path: str):
    """
    Given a datasets, it processes all its phrases line-by-line and
    extracts the wordlist and the hashtag list tuple made like
    ('word or hashtag', count) and returns the two different lists
    :param file_path:
    :return:
    """
    wordlist = []
    file = read_file(file_path)
    stopset = create_stopword_list()
    for phrase in file.splitlines():
        processed_phrase = process_phrase(phrase, stopset)
        wordlist.append(processed_phrase)
    count_tuples = count_words(wordlist)
    most_used_hashtags = count_hashtags(count_tuples)
    count_tuples = exclude_hastags(count_tuples)
    return count_tuples, most_used_hashtags


def quickstart():
    anger_dataset = glob("../Resources/tweets/dataset_dt_anger_60k.txt")
    anticipation_dataset = glob("../Resources/tweets/dataset_dt_anticipation_60k.txt")
    disgust_dataset = glob("../Resources/tweets/dataset_dt_disgust_60k.txt")
    fear_dataset = glob("../Resources/tweets/dataset_dt_fear_60k.txt")
    joy_dataset = glob("../Resources/tweets/dataset_dt_joy_60k.txt")
    sadness_dataset = glob("../Resources/tweets/dataset_dt_sadness_60k.txt")
    surprise_dataset = glob("../Resources/tweets/dataset_dt_surprise_60k.txt")
    trust_dataset = glob("../Resources/tweets/dataset_dt_trust_60k.txt")

    anger_words, anger_hashtags = process_dataset(anger_dataset[0])
    anticipation_words, anticipation_hashtags = process_dataset(anticipation_dataset[0])
    disgust_words, disgust_hashtags = process_dataset(disgust_dataset[0])
    fear_words, fear_hashtags = process_dataset(fear_dataset[0])
    joy_words, joy_hashtags = process_dataset(joy_dataset[0])
    sadness_words, sadness_hashtags = process_dataset(sadness_dataset[0])
    surprise_words, surprise_hashtags = process_dataset(surprise_dataset[0])
    trust_words, trust_hashtags = process_dataset(trust_dataset[0])

    # Strangely, using only 1 thread is faster than using it all
    # this is probably due to using CPU-only functions instead of
    # really relying on I/O operations, but, still strange to my eyes

    """
    all_dataset = glob("../Resources/tweets/*.txt")

    with ThreadPoolExecutor() as executor:
        resulting_table = []
        workerlist = []
        for dataset in all_dataset:
            workerlist.append(executor.submit(process_dataset, dataset))
        for worker in concurrent.futures.as_completed(workerlist):
            print(worker.result())
    """


if __name__ == '__main__':
    download('punkt')
    download('stopwords')
    quickstart()
