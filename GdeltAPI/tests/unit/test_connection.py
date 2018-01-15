import Queue
from struct import pack
import unittest

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.decoder import dict_factory
from cassandra.query import SimpleStatement
from tests.integration.long.utils import create_schema

class QueryServer(unittest.TestCase):

    def setup(self):
        self.keyspace = 'large_data'
    
    def make_session_and_keyspace(self):
        cluster = Cluster()
        session = cluster.connect()
        session.default_timeout = 20.0      # increase the default timeout
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        return session

    def test_wide_rows(self):
        table = 'wide_rows'
