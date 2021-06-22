import string
from timeit import default_timer as timer

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
from wordcloud import STOPWORDS, ImageColorGenerator, WordCloud

from src.file_manager import get_project_root


class WordCloudCreator:
    def __init__(self, dao):
        self.dao = dao

    def generate(self):
        emotions = ['anger', 'anticipation', 'disgust', 'fear', 'joy', 'sadness', 'surprise', 'trust']
        for emotion in emotions:
            self.__generate_emotion_plot(self.dao.get_counts(f'{emotion}_words_frequency'), f'{emotion}')
            if emotion == 'anger' or emotion == 'fear':
                self.__generate_emotion_plot(self.dao.get_counts(f'{emotion}_emoji_frequency'), f'{emotion}_emoji',
                                             emoji=True, extension='png')
            else:
                self.__generate_emotion_plot(self.dao.get_counts(f'{emotion}_emoji_frequency'), f'{emotion}_emoji',
                                             emoji=True)
            self.__generate_emoticon_plot(self.dao.get_counts(f'{emotion}_emoticon_frequency'), f'{emotion}')

    def __preprocess_wordlist(self, wordlist):
        [wordlist.pop(key) for key in self.__get_stopwords() if key in wordlist]
        return wordlist

    @staticmethod
    def __get_stopwords():
        return list(STOPWORDS.union(set(string.punctuation))) + ['..', '...']

    def __generate_emotion_plot(self, wordlist, emotion: str, extension="jpg", emoji=False):
        if '_id' in wordlist:
            # We pop _id from tweets because in the case of MongoDB we cannot iterate
            wordlist.pop('_id')
        print(f"Creating the image for emotion: {emotion}")
        start = timer()
        colored_image = np.array(Image.open(f"{get_project_root()}/Resources/images/{emotion}.{extension}"))
        image_colors = ImageColorGenerator(colored_image)
        if not emoji:
            wordobject = WordCloud(background_color='white',
                                   max_words=2500,
                                   max_font_size=50,
                                   mask=colored_image,
                                   random_state=42,
                                   width=1920,
                                   height=1080)
            plt.figure(figsize=(20, 11.25), dpi=96)
            wordlist = self.__preprocess_wordlist(wordlist)
        else:
            font_path = f"{get_project_root()}/Resources/fonts/symbola.otf"
            wordobject = WordCloud(background_color='black',
                                   max_words=500,
                                   font_path=font_path)
            plt.figure(figsize=(16, 9))
        wordcl = wordobject.generate_from_frequencies(wordlist)
        plt.imshow(wordcl.recolor(color_func=image_colors), interpolation='gaussian')
        plt.axis('off')
        plt.savefig(f"{get_project_root()}/Resources/images/{emotion}_plot.png")
        end = timer()
        print(f"Done creating the image in {end - start} seconds")

    @staticmethod
    def __generate_emoticon_plot(emoticon_list, emotion: str):
        if '_id' in emoticon_list:
            # We pop _id from tweets because in the case of MongoDB we cannot iterate
            emoticon_list.pop('_id')
        print(f"Creating the image for emotion-emoticon: {emotion}")
        start = timer()
        wordobject = WordCloud(background_color='black',
                               max_words=1000,
                               width=1600,
                               height=900)
        plt.figure(figsize=(16, 9))
        wordcl = wordobject.generate_from_frequencies(emoticon_list)
        plt.imshow(wordcl)
        plt.axis('off')
        plt.savefig(f"{get_project_root()}/Resources/images/{emotion}_emoticon_plot.png")
        end = timer()
        print(f"Done creating the image in {end - start} seconds")
