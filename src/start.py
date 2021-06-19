from natural import quickstart
from wordcloudgenerator import WordCloudCreator
from dao.dao import Dao
from timeit import default_timer as timer


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


if __name__ == '__main__':
    dao = start_comparison("MongoDB")
    # dao2 = start_comparison("MySQL")

    wordcl = WordCloudCreator(dao)
    wordcl.generate()
