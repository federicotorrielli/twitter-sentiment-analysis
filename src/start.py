from pprint import pprint
from timeit import default_timer as timer

from dao.dao import Dao
from natural import quickstart, create_word_final_result
from src.graphs import build_histogram_matplotlib
from src.wordcloudgenerator import WordCloudCreator


def start_comparison(db_type="MongoDB"):
    full_time = timer()
    print(f"Building {db_type}...")
    start = timer()
    spec_dao = Dao(db_type != "MongoDB")
    spec_dao.build_db()
    end = timer()
    print(f"Done building {db_type} in {end - start} seconds")

    start = timer()
    quickstart(spec_dao)
    end = timer()
    print(f"Done NLP for {db_type} in {end - start} seconds")
    print(f"Done everything in {end - full_time} seconds")
    return spec_dao


def test_query(db_type="MongoDB"):
    full_time = timer()
    spec_dao = Dao(db_type != "MongoDB")
    # Insert here a good number of queries
    pprint(spec_dao.get_result("angry"))
    pprint(spec_dao.get_result("sus"))
    pprint(spec_dao.get_result("creepy"))
    pprint(spec_dao.get_result("goku"))
    pprint(spec_dao.get_result("!"))
    pprint(spec_dao.get_result("frenzy"))
    end = timer()
    print(f"Done everything in {end - full_time} seconds")


if __name__ == '__main__':
    # TODO: test  MongoDB
    # TODO: compare MySQL and MongoDB data
    # db_type = "MySQL"  # MongoDB / MySQL
    # dao = start_comparison(db_type)
    # test_query(db_type)

    if input("Do you want to generate the Wordclouds? This could take 10 minutes or more! [y/N] ").lower() == "y":
        wordcl = WordCloudCreator(Dao(True))
        wordcl.generate()

    # sentiment_percentages = Dao(True).get_sentiment_percentages()
    # graph_data = {sentiment: sentiment_percentages[sentiment][0] for sentiment in sentiment_percentages}
    # build_histogram_matplotlib(graph_data, '% words [lex resources] in tweets')

