# Python modules
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.query import BatchStatement, ConsistencyLevel

import csv
import re
import datetime
import uuid
#import redis

# Custom User Modules
from models.activity.activity_by_company import ActivityByCompany
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
    # _redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

    
    def connect(self, host, port, keyspace='events'):
        """ Connect with specific host & port """
        connection.setup([host,], "cqlengine", protocol_version=3)
        self._db_connection = Cluster([host,])
        self._db_cur = self._db_connection.connect(keyspace=keyspace)

    def create_connection(self, keyspace):
        """Create connection with specified keyspace"""

        self._db_connection = Cluster()
        self._db_cur = self._db_connection.connect()
        self._keyspace = keyspace

        self._db_cur.execute("""
            DROP KEYSPACE IF EXISTS %s
        """ % self._keyspace)

        self._db_cur.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'SimpleStrategy',
                                                            'replication_factor': 1};
        """ % self._keyspace)
        self._db_cur = self._db_connection.connect(keyspace=self._keyspace)

    def query(self, query):
        """ query cassandra db """
        return self._db_cur.execute(query)

    def sync_db(self):
        """ create tables as models inside self._tables_to_sync """
        for table in self._tables_to_sync:
            sync_table(table)

    def check_time(self,date_string):
        return re.match(r"\d{4}/\d{2}/\d{2}", date_string)

    def update_event_by_company(self, event, company, date):
        """ update activity_by_company with event={view/click/subscribe} for a company"""
        if not event:
            return

        event = event+"s" # make it plural
        update_event = "UPDATE ACTIVITY_BY_COMPANY SET {0} = {0} + 1 where company = '{1}' AND date = '{2}'".format(event, company, date)

        self._db_cur.execute(update_event)

    def get_company(self, user_id):
        company = self._redis_db.get(user_id)
        if company is None:
            company = "NULL"
        return company

    def import_from_activity(self, filename):
        """ import from csv file to db """
        f = open(filename)

        # read the csv fle and skip th next header
        csv_f = csv.reader(f)
        next(csv_f, None)

        # insert ActivityByUser
        insert_count_activity_by_user = self._db_cur.prepare("UPDATE COUNT_ACTIVITY_BY_USER SET counter = counter + 1 where date = ? and company = ? ")
        insert_activity_by_user = self._db_cur.prepare("INSERT INTO ACTIVITY_BY_USER (date, event, domain, id, url, user_id, datetime) VALUES(?, ?, ?, ?, ?, ?, ? )")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for row in csv_f:
            if len(row) != 4:
                continue
            (user_id, event, url, time_stamp) = row
            domain = re.findall('https?://[^/]*',url, re.IGNORECASE)

            if len(domain) != 1:
                domain = ""
            else:
                domain = domain[0][7:]
                if domain[0] == '/':
                    domain = domain[1:]

            # if not correct time format. continue
            if( not self.check_time(time_stamp)):
                continue

            try:
                dt = datetime.datetime.strptime(time_stamp, "%Y/%m/%d %H:%M:%S.%f")    
            except ValueError:
                dt = datetime.datetime.strptime(time_stamp, "%Y/%m/%d %H:%M:%S")

            while len(batch) > self._batch:
                self._db_cur.execute(batch)
                batch.clear()
            
            bucket = dt.strftime("%Y/%m/%d")
            company = self.get_company(user_id)
            batch.add(insert_activity_by_user,(bucket, event, domain, uuid.uuid1(), url, int(user_id), dt))
            self._db_cur.execute(insert_count_activity_by_user, (bucket, company))
            self.update_event_by_company(event, company, bucket)
        
        if len(batch) != 0:
            self._db_cur.execute(batch)


    def import_from_file(self, filename):
        """ import data from csv to db """
        f = open(filename)

        # read the csv file and skip the next header
        csv_f = csv.reader(f)
        next(csv_f, None)
        # insert usersById
        insert_user_by_id = self._db_cur.prepare("INSERT INTO USER_BY_ID (id,first_name, last_name, email, company, city) VALUES ( ?, ?, ?, ?, ?, ? )")
        insert_user_by_email = self._db_cur.prepare("INSERT INTO USER_BY_EMAIL (id,first_name, last_name, email, company, city) VALUES ( ?, ?, ?, ?, ?, ? )")
        insert_user_by_company_city = self._db_cur.prepare("INSERT INTO USER_BY_COMPANY_CITY (id,first_name, last_name, email, company, city) VALUES ( ?, ?, ?, ?, ?, ? )")
        update_user_by_domain = self._db_cur.prepare("UPDATE USER_BY_DOMAIN SET counter = counter + 1 WHERE domain = ? AND city = ?")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        for iid, first_name, last_name, email, company , city in csv_f:
            while len(batch) > self._batch * len(self._tables_to_sync):
                self._db_cur.execute(batch)
                batch.clear()
            # insert into UserById table
            batch.add(insert_user_by_id, (int(iid), first_name, last_name, email, company, city))
            batch.add(insert_user_by_email, (int(iid), first_name, last_name, email, company, city))
            batch.add(insert_user_by_company_city, (int(iid), first_name, last_name, email, company, city))

            # get the email domain
            domain = re.search('@(.*)$',email, re.IGNORECASE).group()

            self._db_cur.execute(update_user_by_domain, (domain[1:], city))

            # Save company name to redis
            self._redis_db.set(iid, company)

        if len(batch) != 0:
            self._db_cur.execute(batch)


    