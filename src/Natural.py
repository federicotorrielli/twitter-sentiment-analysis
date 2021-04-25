#!/usr/bin/python3
from glob import glob
import re

from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

from FileManager import read_file
from Resources.lexical.set_classification import twitter_stopwords, posemoticons, negemoticons

# TODO: check reduce_len and preserve_case
tokenizer = TweetTokenizer(reduce_len=True)


def clean_emoticons(phrase: str):
    emoticon_list = [emoticon for emoticon in (posemoticons + negemoticons) if emoticon in phrase]
    for emote in emoticon_list:
        phrase = phrase.replace(emote, '')
    return phrase, emoticon_list


def process_phrase(phrase: str, stopword_list: set):
    clean_phrase, emote_list = clean_emoticons(phrase)
    tokenized = tokenizer.tokenize(clean_phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list] + emote_list
    print(final_phrase)


def create_stopword_list() -> set:
    default = stopwords.words('english')
    personal = twitter_stopwords
    final = default + personal
    return set(final)


def process_dataset(file_path: str):
    file = read_file(file_path)
    stopset = create_stopword_list()
    for phrase in file.splitlines():
        process_phrase(phrase, stopset)


def quickstart():
    # TODO: set the resources to *.txt once it's GTG
    num_datasets = glob("../Resources/tweets/dataset_dt_anger_60k.txt")
    for dataset in num_datasets:
        process_dataset(dataset)


if __name__ == '__main__':
    download('punkt')
    download('stopwords')
    quickstart()
