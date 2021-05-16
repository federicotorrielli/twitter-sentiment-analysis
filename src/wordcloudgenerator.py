import string
from timeit import default_timer as timer

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
from wordcloud import STOPWORDS, ImageColorGenerator, WordCloud

from src.file_manager import get_project_root


class WordCloudCreator:
    def __init__(self, wordlist: []):
        self.wordlist = wordlist

    def generate(self):
        self.__generate_emotion_plot(dict(self.wordlist[0]), 'anger')
        self.__generate_emotion_plot(dict(self.wordlist[1]), 'anticipation')
        self.__generate_emotion_plot(dict(self.wordlist[2]), 'disgust')
        self.__generate_emotion_plot(dict(self.wordlist[3]), 'fear')
        self.__generate_emotion_plot(dict(self.wordlist[4]), 'joy')
        self.__generate_emotion_plot(dict(self.wordlist[5]), 'sadness')
        self.__generate_emotion_plot(dict(self.wordlist[6]), 'surprise')
        self.__generate_emotion_plot(dict(self.wordlist[7]), 'trust')

    def __preprocess_wordlist(self, wordlist):
        [wordlist.pop(key) for key in self.__get_stopwords() if key in wordlist]
        return wordlist

    @staticmethod
    def __get_stopwords():
        return list(STOPWORDS.union(set(string.punctuation))) + ['..', '...']

    def __generate_emotion_plot(self, wordlist, emotion: str):
        print(f"Creating the image for emotion: {emotion}")
        start = timer()
        colored_image = np.array(Image.open(f"{get_project_root()}/Resources/images/{emotion}.jpg"))
        image_colors = ImageColorGenerator(colored_image)
        wordobject = WordCloud(background_color='white',
                               max_words=2500,
                               mask=colored_image,
                               max_font_size=40,
                               random_state=42)
        wordlist = self.__preprocess_wordlist(wordlist)
        wordcl = wordobject.generate_from_frequencies(wordlist)
        plt.figure(figsize=(20, 11.25), dpi=96)
        plt.imshow(wordcl.recolor(color_func=image_colors), interpolation='bilinear')
        plt.axis('off')
        plt.savefig(f"{get_project_root()}/Resources/images/{emotion}_plot.jpg")
        end = timer()
        print(f"Done creating the image in {end - start} seconds")
