import boto3
from botocore.client import Config
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

    def is_events(self):
        return self.name_ == 'export'


class S3Extractor:
    def __init__(self, sectors, s3_prefix):
        self.bucket_ = 'telecom.gdelt'
        self.sectors_ = sectors
        config = Config(connect_timeout=240, read_timeout=240)
        self.client_ = boto3.client('s3', config=config)
        self.s3_prefix_ = s3_prefix

    def get_entities(self, input_zip, csv_config):
        input_zip = ZipFile(input_zip)
        all_files = []
        for name in input_zip.namelist():
            csv_file = BytesIO(input_zip.read(name))
            frame = pd.read_csv(csv_file, header=None, delimiter='\t', usecols=csv_config.column_indexes_,
                                names=csv_config.column_names_, index_col=[0])
            all_files.append(frame)
        if len(all_files) > 1:
            return pd.concat(all_files)
        return all_files[0]

    def read_body_safe(self, body):
        attempts_nb = 3
        result = None
        while attempts_nb > 0:
            try:
                result = body.read()
            except:
                attempts_nb -= 1
            if result is not None:
                return result
        return None

    def extract_entities(self):
        csv_config = self.get_config()
        print('Extracting ' + csv_config.name_)
        count = 0
        all_files = []
        marker = self.s3_prefix_ + '01000000.' + csv_config.name_ + '.CSV.zip'
        is_truncated = True
        while is_truncated:
            response = self.client_.list_objects(Bucket=self.bucket_, Marker=marker, Prefix=self.s3_prefix_)
            objects = response['Contents']
            for obj in objects:
                if not obj['Key'].endswith(csv_config.name_ + '.CSV.zip'):
                    continue
                count += 1
                if count % 10 == 0:
                    print(count)
                body = self.client_.get_object(Bucket=self.bucket_, Key=obj['Key'])['Body']
                body_content = self.read_body_safe(body)
                if body_content is None:
                    continue
                in_memory_zip = BytesIO(body_content)
                df = self.get_entities(in_memory_zip, csv_config)
                df = self.clean_up_transform(df)
                all_files.append(df)
            marker = objects[-1]['Key']
            is_truncated = response['IsTruncated']
        return pd.concat(all_files)


class EventS3Extractor(S3Extractor):
    def __init__(self, sectors, s3_prefix):
        super().__init__(sectors, s3_prefix)

    def get_config(self):
        config = CsvConfiguration()
        config.set_name('export')
        config.set_column_indexes([0, 1, 5, 6, 7, 60])
        config.set_column_names(['GlobalEventId', 'EventDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
                                 'SourceUrl'])
        config.set_dtypes({"GlobalEventId": np.int, "EventDate": np.str, "Actor1Code": np.str, "Actor1Name": np.str,
                           "Actor1CountryCode": np.str, "SourceUrl": np.str})
        return config

    def get_sector(self, firm):
        if firm in self.sectors_:
            return self.sectors_[firm]
        return 'Other'

    def clean_up_transform(self, df):
        df = df[df['Actor1Code'] == 'MNCUSAMED']
        df.dropna(subset=['EventDate', 'Actor1Name', 'SourceUrl'], inplace=True)
        df['sector'] = df['Actor1Name'].apply(self.get_sector)
        df.rename(columns={'Actor1Name': 'firm', 'Actor1CountryCode': 'country'}, inplace=True)
        return df


class MentionsS3Extractor(S3Extractor):
    def __init__(self, sectors, s3_prefix):
        super().__init__(sectors, s3_prefix)

    def get_config(self):
        config = CsvConfiguration()
        config.set_name('mentions')
        config.set_column_indexes([0, 2, 4, 11, 13])
        config.set_column_names(['GlobalEventId', 'MentionDateTime', 'MentionSourceName', 'Confidence',
                                 'MentionDocTone'])
        config.set_dtypes({"GlobalEventId": np.int, "MentionDateTime": np.str, "MentionSourceName": np.str,
                           "Confidence": np.int8, "MentionDocTone": np.float32})
        return config

    def clean_up_transform(self, df):
        df.dropna(inplace=True)
        return df


class EventMentionsExtractor:
    def extract(self, sectors, s3_prefix):
        events_extractor = EventS3Extractor(sectors, s3_prefix)
        events_df = events_extractor.extract_entities()
        mentions_extractor = MentionsS3Extractor(sectors, s3_prefix)
        mentions_df = mentions_extractor.extract_entities()
        print('Joining events and mentions')
        result = events_df.join(mentions_df)
        result.dropna(inplace=True)
        return result
