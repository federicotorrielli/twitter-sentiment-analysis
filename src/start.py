from pprint import pprint
from timeit import default_timer as timer

from dao.dao import Dao
from natural import quickstart
from wordcloudgenerator import WordCloudCreator


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
    dao = start_comparison("MongoDB")
    # dao2 = start_comparison("MySQL")

    test_query()
    test_query("MySQL")

    # TODO: Wordclouds from MySQL too
    if input("Do you want to generate the Wordclouds? This could take 10 minutes or more! [y/N] ").lower() == "y":
        wordcl = WordCloudCreator(dao)
        wordcl.generate()

