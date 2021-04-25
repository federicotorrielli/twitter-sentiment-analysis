from nltk import download
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from glob import glob
from FileManager import *
from Resources.lexical.set_classification import *

# TODO: check reduce_len and preserve_case
tokenizer = TweetTokenizer(reduce_len=True, preserve_case=False)


def process_phrase(phrase: str, stopword_list: set):
    tokenized = tokenizer.tokenize(phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list]
    # TODO: tokenize emoticons correctly and continue the program
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
    num_datasets = glob("../Resources/tweets/dataset_dt_anger_60k.txt")
    for dataset in num_datasets:
        process_dataset(dataset)


if __name__ == '__main__':
    download('punkt')
    download('stopwords')
    quickstart()
