from natural import quickstart
from wordcloudgenerator import WordCloudCreator
from dao.dao import Dao

if __name__ == '__main__':
    # First, we start the NLP module that will produce the content
    word_datasets, emoji_datasets, emoticons_datasets = quickstart()
    # TODO: quickstart the mysql db here
    # TODO: pass these values to the mongodb server
    dao = Dao(False, word_datasets, emoji_datasets, emoticons_datasets)
    dao.build_db()

    # TODO: make the wordcloud take the datasets directly from mongodb
    # wordcl = WordCloudCreator(word_datasets, emoji_datasets, emoticons_datasets)
    # wordcl.generate()
