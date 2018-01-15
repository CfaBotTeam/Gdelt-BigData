__author__ = 'puneet'

from dao.db import CassandraDBConnector


KEYSPACE = 'granify'
USERS_FILE_NAME = 'data/users.csv'
ACTIVITY_FILE_NAME = 'data/activity.csv'

def sync_table():
    db = CassandraDBConnector()
    db.create_connection(KEYSPACE)

    # Sync db using cassandra manager 
    db.connect("127.0.0.1", "1234")
    db.sync_db()

    # import csv file
    db.import_from_file(USERS_FILE_NAME)
    db.import_from_activity(ACTIVITY_FILE_NAME)

if __name__ == '__main__':
    sync_table()