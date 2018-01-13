import boto3, re
import pandas as pd
from io import BytesIO, StringIO
from zipfile import ZipFile


class S3Extractor:
    def __init__(self):
        self.s3_ = boto3.resource('s3')
        self.buck_ = self.s3_.Bucket('telecom.gdelt')

    def add_entities(self, input_zip, all_files, column_indexes, column_names):
        input_zip = ZipFile(input_zip)
        for name in input_zip.namelist():
            csv_file = BytesIO(input_zip.read(name))
            frame = pd.read_csv(csv_file, header=None, delimiter='\t', usecols=column_indexes, names=column_names)
            all_files.append(frame)

    def extract_entities(self, entity, column_indexes, column_names):
        all_files = []
        count = 0
        for obj in self.buck_.objects.all():
            if not obj.key.endswith(entity + '.CSV.zip'):
                continue

            if count > 10:
                break
            count += 1

            in_memory_zip = BytesIO(obj.get()['Body'].read())
            self.add_entities(in_memory_zip, all_files, column_indexes, column_names)
        return pd.concat(all_files, ignore_index=True)

    def extract_mentions(self):
        events_df = self.extract_entities('export', [0, 5, 6, 60], ['GlobalEventId', 'Actor1Code', 'Actor1Name', 'SourceUrl'])
        mentions_df = self.extract_entities('mentions', [0, 2, 4, 11, 13], ['GlobalEventId', 'MentionTimeDate', 'MentionSourceName', 'Confidence', 'MentionDocTone'])
        events_df.set_index('GlobalEventId', inplace=True)
        mentions_df.set_index('GlobalEventId', inplace=True)
        return pd.concat((events_df, mentions_df), axis=1, join_axes=[mentions_df.index])


class Cassandra:
    pass


extractor = S3Extractor()
event_mentions = extractor.extract_mentions()
pass