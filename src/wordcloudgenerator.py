import string
from timeit import default_timer as timer

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
from wordcloud import STOPWORDS, ImageColorGenerator, WordCloud

from src.file_manager import get_project_root


class WordCloudCreator:
    def __init__(self, wordlist: [], emoji_list: [], emoticon_list: []):
        self.wordlist = wordlist
        self.emoji_list = emoji_list
        self.emoticon_list = emoticon_list

    def generate(self):
        self.__generate_emotion_plot(dict(self.wordlist[0]), 'anger')
        self.__generate_emotion_plot(dict(self.wordlist[1]), 'anticipation')
        self.__generate_emotion_plot(dict(self.wordlist[2]), 'disgust')
        self.__generate_emotion_plot(dict(self.wordlist[3]), 'fear')
        self.__generate_emotion_plot(dict(self.wordlist[4]), 'joy')
        self.__generate_emotion_plot(dict(self.wordlist[5]), 'sadness')
        self.__generate_emotion_plot(dict(self.wordlist[6]), 'surprise')
        self.__generate_emotion_plot(dict(self.wordlist[7]), 'trust')
        self.__generate_emotion_plot(dict(self.emoji_list[0]), 'anger_emoji', extension='png', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[1]), 'anticipation_emoji', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[2]), 'disgust_emoji', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[3]), 'fear_emoji', extension='png', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[4]), 'joy_emoji', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[5]), 'sadness_emoji', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[6]), 'surprise_emoji', emoji=True)
        self.__generate_emotion_plot(dict(self.emoji_list[7]), 'trust_emoji', emoji=True)
        self.__generate_emoticon_plot(dict(self.emoticon_list[0]), 'anger')
        self.__generate_emoticon_plot(dict(self.emoticon_list[1]), 'anticipation')
        self.__generate_emoticon_plot(dict(self.emoticon_list[2]), 'disgust')
        self.__generate_emoticon_plot(dict(self.emoticon_list[3]), 'fear')
        self.__generate_emoticon_plot(dict(self.emoticon_list[4]), 'joy')
        self.__generate_emoticon_plot(dict(self.emoticon_list[5]), 'sadness')
        self.__generate_emoticon_plot(dict(self.emoticon_list[6]), 'surprise')
        self.__generate_emoticon_plot(dict(self.emoticon_list[7]), 'trust')

    def __preprocess_wordlist(self, wordlist):
        [wordlist.pop(key) for key in self.__get_stopwords() if key in wordlist]
        return wordlist

    @staticmethod
    def __get_stopwords():
        return list(STOPWORDS.union(set(string.punctuation))) + ['..', '...']

    def __generate_emotion_plot(self, wordlist, emotion: str, extension="jpg", emoji=False):
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
