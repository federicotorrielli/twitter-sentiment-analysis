from natural import quickstart
from wordcloudgenerator import WordCloudCreator
from dao.dao import Dao
from timeit import default_timer as timer

if __name__ == '__main__':
    # TODO: quickstart the mysql db here

    print("Building MongoDB...")
    start = timer()
    dao_mongo = Dao(False)
    dao_mongo.build_db()
    end = timer()
    print(f"Done building MongoDB in {end - start} seconds")

    start = timer()
    quickstart("mongo")
    end = timer()
    print(f"Done NLP for MongoDB in {end - start} seconds")

    wordcl = WordCloudCreator(dao_mongo)
    wordcl.generate()
