from downloads_mentions import EventMentionsExtractor
from cassandra_persister import CassandraPersister
from sectors_loader import SectorsLoader
import pandas as pd

pd.options.mode.chained_assignment = None

loader = SectorsLoader()
sectors = loader.load_company_sectors()
extractor = EventMentionsExtractor()
persister = CassandraPersister()

months = [31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


for i, month in enumerate(months):
    print('Month index = ' + str(i))
    days1 = ['0' + str(x) for x in range(1, 10)]
    days2 = [str(x) for x in range(10, month + 1)]
    days = days1 + days2
    for day in days:
        prefix = '201701' + day
        print('Export / Import for ' + prefix)
        events_mentions = extractor.extract(sectors, prefix)
        events_mentions.to_csv('events_mentions_201701' + day + '.csv', sep=',')
        persister.save_events_mentions(events_mentions)
