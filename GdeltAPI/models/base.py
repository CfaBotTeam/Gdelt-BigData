from cassandra.cqlengine.models import Model


__author__ = 'puneet'


class Base(Model):
    __abstract__ = True
    __keyspace__ = "events"