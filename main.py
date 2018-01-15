from downloads_mentions import EventMentionsExtractor
from cassandra_persister import CassandraPersister
from sectors_loader import SectorsLoader
import pandas as pd

pd.options.mode.chained_assignment = None

loader = SectorsLoader()
sectors = loader.load_company_sectors()
extractor = EventMentionsExtractor()
persister = CassandraPersister()

months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


for i, month_max in enumerate(months):
    if i < 11:
        # we skip the first month already done
        continue
    month_index = i + 1
    month = str(month_index)
    if month_index < 10:
        month = '0' + month
    print('Month = ' + str(month))
    #days1 = ['0' + str(x) for x in range(1, 10)]
    days2 = [str(x) for x in range(16, month_max)]
    days = days2
    for day in days:
        prefix = '2017' + month + day
        print('Export / Import for ' + prefix)
        events_mentions = extractor.extract(sectors, prefix)
        events_mentions.to_csv('events_mentions_201701' + day + '.csv', sep=',')
        persister.save_events_mentions(events_mentions)
