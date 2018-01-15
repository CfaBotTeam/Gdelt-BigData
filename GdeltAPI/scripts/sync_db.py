__author__ = 'puneet'

from cassandra.cqlengine import connection
from dao.db import CassandraDBConnector

KEYSPACE = 'granify'

def sync_table():
    connection.setup(['127.0.0.1'], "cqlengine", protocol_version=3)
    db = CassandraDBConnector()
    #db.create_connection(KEYSPACE)
    db.sync_db()

if __name__ == '__main__':
    sync_table()