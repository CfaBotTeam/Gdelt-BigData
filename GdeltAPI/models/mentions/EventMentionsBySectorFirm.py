from cassandra.cqlengine import columns
from models.base import Base


class EventsMentionsBySectorFirm(Base):
    global_event_id = columns.Integer()
    firm = columns.Text(partition_key=True)
    sector = columns.Text(partition_key=True)
    mention_source_name = columns.Text()
    country = columns.Text()
    confidence = columns.Integer()
    mention_doc_tone = columns.Float()
    event_date = columns.Date()
    mention_date_time = columns.DateTime()

    def get_date(self):
        return {
            "globalEventsId": self.global_event_id,
            "firm": self.firm,
            "sector": self.sector,
            "mentionSourceName": self.mention_source_name,
            "country": self.country,
            "confidence": self.confidence,
            "mentionDocTone": self.mention_doc_tone,
            "eventDate": self.event_date,
            "mentionDateTime": self.mention_date_time
        }
