import boto3, re
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile


class CsvConfiguration:
    def __init__(self):
        self.name_ = ''
        self.column_indexes_ = []
        self.column_names_ = []
        self.dtypes_ = []

    def set_name(self, name):
        self.name_ = name

    def set_column_indexes(self, column_indexes):
        self.column_indexes_ = column_indexes

    def set_column_names(self, column_names):
        self.column_names_ = column_names

    def set_dtypes(self, dtypes):
        self.dtypes_ = dtypes


class S3Extractor:
    def __init__(self):
        self.s3_ = boto3.resource('s3')
        self.buck_ = self.s3_.Bucket('telecom.gdelt')

    def get_entities(self, input_zip, csv_config):
        input_zip = ZipFile(input_zip)
        all_files = []
        for name in input_zip.namelist():
            csv_file = BytesIO(input_zip.read(name))
            frame = pd.read_csv(csv_file, header=None, delimiter='\t', usecols=csv_config.column_indexes_,
                                names=csv_config.column_names_, index_col=0)
            frame.dropna(inplace=True)
            all_files.append(frame)
        if len(all_files) > 1:
            return pd.concat(all_files, ignore_index=True)
        return all_files[0]

    def extract_entities(self, csv_config):
        count = 0
        all_files = []
        for obj in self.buck_.objects.all():
            if not obj.key.endswith(csv_config.name_ + '.CSV.zip'):
                continue

            if count > 10:
                break
            count += 1

            in_memory_zip = BytesIO(obj.get()['Body'].read())
            df = self.get_entities(in_memory_zip, csv_config)
            all_files.append(df)
        return pd.concat(all_files)

    def get_events_config(self):
        config = CsvConfiguration()
        config.set_name('export')
        config.set_column_indexes([0, 1, 5, 6, 7, 60])
        config.set_column_names(['GlobalEventId', 'EventDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
                                 'SourceUrl'])
        config.set_dtypes({"GlobalEventId": np.str, "EventDate": np.str, "Actor1Code": np.str, "Actor1Name": np.str,
                           "Actor1CountryCode": np.str, "SourceUrl": np.str})
        return config

    def get_mentions_config(self):
        config = CsvConfiguration()
        config.set_name('mentions')
        config.set_column_indexes([0, 2, 4, 11, 13])
        config.set_column_names(['GlobalEventId', 'MentionDateTime', 'MentionSourceName', 'Confidence',
                                 'MentionDocTone'])
        config.set_dtypes({"GlobalEventId": np.str, "MentionDateTime": np.str, "MentionSourceName": np.str,
                           "Confidence": np.int8, "MentionDocTone": np.float32})
        return config

    def extract_mentions(self):
        events_config = self.get_events_config()
        events_df = self.extract_entities(events_config)
        mentions_config = self.get_mentions_config()
        mentions_df = self.extract_entities(mentions_config)
        return pd.concat((events_df, mentions_df), axis=1, join_axes=[mentions_df.index])


class Cassandra:
    pass


extractor = S3Extractor()
event_mentions = extractor.extract_mentions()
print(event_mentions.isnull().sum())
event_mentions.dropna(inplace=True)
print(event_mentions.isnull().sum())
pass