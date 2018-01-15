from cassandra.cluster import Cluster, SimpleStatement, BatchStatement, ConsistencyLevel


class CassandraPersister:
    def __init__(self):
        self.cluster_ = Cluster(["ec2-54-160-15-115.compute-1.amazonaws.com",
                                 "ec2-34-207-179-124.compute-1.amazonaws.com",
                                 "ec2-174-129-101-198.compute-1.amazonaws.com"], connect_timeout=20.0)

    def format_date(self, date):
        return "{0}-{1}-{2}".format(date[:4], date[4:6], date[6:8])

    def format_timestamp(self, date):
        return "{0}-{1}-{2} {3}:{4}:{5}".format(date[:4], date[4:6], date[6:8], date[8:10], date[10:12], date[12:14])

    def get_statement(self, row_index, events_mentions):
        mentions = events_mentions.iloc[row_index]
        global_events_id = events_mentions.index[row_index]
        try:
            formatted_mention_timestamp = self.format_timestamp(str(mentions.MentionDateTime))
        except:
            print('Unable to format mention timestamp: ' + str(mentions.MentionDateTime))
            return None
        try:
            formatted_event_date = self.format_date(str(mentions.EventDate))
        except:
            print('Unable to format event date: ' + str(mentions.EventDate))
            return None
        query = "INSERT INTO events_mentions" + \
                " (globalEventsId , eventDate, mentionDateTime," + \
                " mentionSourceName, confidence , mentionDocTone," + \
                " firm, sector, country) " + \
                "VALUES ({0}, '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}')".format(
                global_events_id, formatted_event_date,
                formatted_mention_timestamp, mentions.MentionSourceName,
                mentions.Confidence, mentions.MentionDocTone,
                mentions.firm, mentions.sector,
                mentions.country)
        return SimpleStatement(query)

    def save_events_mentions(self, events_mentions):
        session = self.cluster_.connect('events')
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for row_index in range(len(events_mentions)):
            statement = self.get_statement(row_index, events_mentions)
            if statement is None:
                continue
            batch.add(statement)
        session.execute(batch)
