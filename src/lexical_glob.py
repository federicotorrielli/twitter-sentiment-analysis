from glob import glob
from pprint import pprint

from FileManager import count_file_lines, read_file
from src.set_classification import posemoticons, negemoticons, EmojiPos, EmojiNeg, OthersEmoji


def get_lexical_filenames():
    """
    Foreach Plutchik emotion, it returns the lexical filenames.
    @return: lexical filenames
    """
    return [glob("../Resources/lexical/Anger/*.txt"), glob("../Resources/lexical/Anticipation/*.txt"),
            glob("../Resources/lexical/Disgust-Hate/*.txt"), glob("../Resources/lexical/Fear/*.txt"),
            glob("../Resources/lexical/Joy/*.txt"), glob("../Resources/lexical/Sadness/*.txt"),
            glob("../Resources/lexical/Surprise/*.txt"), glob("../Resources/lexical/Trust/*.txt")]


def get_lexical_Nlines():
    """
    Foreach Plutchik emotion, it finds # words.
    @return: list of # sentiment words
    """
    sumlist = []
    for index, files in enumerate(get_lexical_filenames()):
        sumlist.append(0)
        for file in files:
            sumlist[index] += count_file_lines(file)
    return sumlist


# TODO check if functions below should be here

def get_sentiment_tweets():
    """
    Returns a list of tweets divided per sentiment
    @return: list of tweets divided per sentiment
    """
    anger_dataset = glob("../Resources/tweets/dataset_dt_anger_60k.txt")
    anticipation_dataset = glob("../Resources/tweets/dataset_dt_anticipation_60k.txt")
    disgust_dataset = glob("../Resources/tweets/dataset_dt_disgust_60k.txt")
    fear_dataset = glob("../Resources/tweets/dataset_dt_fear_60k.txt")
    joy_dataset = glob("../Resources/tweets/dataset_dt_joy_60k.txt")
    sadness_dataset = glob("../Resources/tweets/dataset_dt_sadness_60k.txt")
    surprise_dataset = glob("../Resources/tweets/dataset_dt_surprise_60k.txt")
    trust_dataset = glob("../Resources/tweets/dataset_dt_trust_60k.txt")

    return [anger_dataset, anticipation_dataset, disgust_dataset, fear_dataset, joy_dataset, sadness_dataset,
            surprise_dataset, trust_dataset]


def get_sentiment_words():
    """
    Returns words and their sentiments
    @return: a dict with words as keys and list of word sentiments' ids as values
    """
    words_list = []
    for files in get_lexical_filenames():
        for file in files:
            for word in read_file(file).splitlines():
                words_list.append(word)

    word_sentiments = {word.lower(): [] for word in sorted(list(dict.fromkeys(words_list)))}

    for index, files in enumerate(get_lexical_filenames()):
        for file in files:
            for word in read_file(file).splitlines():
                if index not in word_sentiments[f"{word.lower()}"]:
                    word_sentiments[f"{word.lower()}"] += [index]
    return word_sentiments


def get_sentiment_emoticons():
    """
    Returns positive and negative emoticons
    @return: a dict with positive/negative as keys and list of emoticons as values
    """
    return {'positive': [emoticon for emoticon in posemoticons],
            'negative': [emoticon for emoticon in negemoticons]}


def get_sentiment_emoji():
    """
    Returns positive and negative emoji
    @return: a dict with positive/negative/other/neutral as keys and list of emoji as values
    """
    return {'positive': [emoji for emoji in EmojiPos],
            'negative': [emoji for emoji in EmojiNeg],
            'other': [emoji for emoji in OthersEmoji]}


if __name__ == '__main__':
    pprint(get_lexical_Nlines())
