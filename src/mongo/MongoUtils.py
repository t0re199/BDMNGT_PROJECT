import math
from pymongo import MongoClient


__MONGO_IP__ = "127.0.0.1"
__MONGO_PORT__ = 27017
__MONGO_INSERT_MANY_LIMIT__ = 1000


DB_NAME = "BDMNT_DB"
COLLECTIONS_NAMES = ["query0", "query1", "query2"]


def open_connection():
    return MongoClient(port=__MONGO_PORT__)


def close_connection(mongo_client):
    mongo_client.close()


def create_db(db_name):
    mongo_client = MongoClient(port=__MONGO_PORT__)

    db = mongo_client[db_name]

    for collection_name in COLLECTIONS_NAMES:
        _ = db[collection_name]

    mongo_client.close()


def drop_db(db_name):
    mongo_client = MongoClient(port=__MONGO_PORT__)

    mongo_client.drop_database(db_name)

    mongo_client.close()


def insert_many_to(db_name, collection_name, data, limit=__MONGO_INSERT_MANY_LIMIT__):
    mongo_client = MongoClient(port=__MONGO_PORT__)

    db = mongo_client[db_name]
    collection = db[collection_name]

    end = len(data)
    chunks = math.ceil(end / limit)

    for chunk in range(chunks):
        collection.insert_many(data[chunk * limit: min((chunk + 0x1) * limit, end)])

    mongo_client.close()
