from nltk import tokenize, download
from nltk.corpus import stopwords
from glob import glob
from FileManager import *


def process_phrase(phrase: str, stopword_list: set):
    tokenized = tokenize.word_tokenize(phrase)
    final_phrase = [word for word in tokenized if word not in stopword_list]
    print(final_phrase)


def create_stopword_list() -> set:
    default = stopwords.words('english')
    return set(default)


def process_dataset(file_path: str):
    file = read_file(file_path)
    stopset = create_stopword_list()
    for phrase in file.splitlines():
        process_phrase(phrase, stopset)


def quickstart():
    num_datasets = glob("../Resources/twitter/dataset_dt_anger_60k.txt")
    for dataset in num_datasets:
        process_dataset(dataset)


if __name__ == '__main__':
    download('punkt')
    download('stopwords')
    quickstart()
