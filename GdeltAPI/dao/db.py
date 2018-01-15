# Python modules
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


# Custom User Modules
from models.mentions.EventMentionsBySectorFirm import EventsMentionsBySectorFirm

__author__ = 'Puneet Girdhar'

def Singleton(cls):
    """ Create Singleton class"""

    instances = {}
    def getInstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getInstance


@Singleton
class CassandraDBConnector:
    """ Cassandra db connector """
    _db_cur = None
    _tables_to_sync = [EventsMentionsBySectorFirm]
    _batch = 10
    
    def connect(self, host, port, keyspace='events'):
        """ Connect with specific host & port """
        connection.setup([host,], "cqlengine", protocol_version=3)
        self._db_connection = Cluster([host,])
        self._db_cur = self._db_connection.connect(keyspace=keyspace)

    def query(self, query):
        """ query cassandra db """
        return self._db_cur.execute(query)
