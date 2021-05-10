from glob import glob

from emoji import UNICODE_EMOJI_ENGLISH

from src.file_manager import read_file
from src.lexical_glob import get_lexical_filenames
from src.set_classification import EmojiPos, EmojiNeg, OthersEmoji, posemoticons, negemoticons


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


def get_sentiment_emojis():
    """
    Returns positive and negative emoji
    @return: a dict with positive/negative/other/neutral as keys and list of emoji as values
    """
    return {'positive': [emoji for emoji in EmojiPos],
            'negative': [emoji for emoji in EmojiNeg],
            'other': [emoji for emoji in set(OthersEmoji + (UNICODE_EMOJI_ENGLISH - EmojiPos - EmojiNeg))]}

