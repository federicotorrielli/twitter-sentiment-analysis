#!/usr/bin/python3
from collections import Counter
from glob import glob

from emoji import UNICODE_EMOJI_ENGLISH
from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

from FileManager import read_file
from set_classification import negemoticons, posemoticons, twitter_stopwords
from slang import create_definitions

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
    return [item for item in count_tuples.most_common() if item[0].startswith('#')]


def count_emojis(count_tuples):
    return [item for item in count_tuples.most_common() if item[0] in UNICODE_EMOJI_ENGLISH
            or item[0] in posemoticons or item[0] in negemoticons]


def exclude_hastags_emojis(count_tuples):
    return [item for item in count_tuples.most_common() if not item[0].startswith('#')
            and not item[0] in UNICODE_EMOJI_ENGLISH and not item[0] in posemoticons and not item[0] in negemoticons]


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
    emojis = count_emojis(count_tuples)
    count_tuples = exclude_hastags_emojis(count_tuples)
    return count_tuples, most_used_hashtags, emojis


def quickstart():
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

    create_definitions(word_datasets)

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
