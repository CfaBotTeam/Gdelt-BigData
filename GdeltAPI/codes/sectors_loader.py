import pandas as pd
import numpy as np


class SectorsLoader:
    def __init__(self):
        self.sectors_ = {}
        self.unique_sectors_ = []
        self.nb_company_ = 0
        self.nb_sectors_ = 0

    def load_company_sectors(self):
        sectors_df = pd.read_csv('codes/company_codes.csv')
        for row_index in range(len(sectors_df)):
            company_name = sectors_df.iloc[row_index].Actor1Name
            sector = sectors_df.iloc[row_index].Sector
            self.sectors_[company_name] = sector

    def get_sector(self, company_name):
        if company_name in self.sectors_:
            return self.sectors_[company_name]
        return None

    def get_sectors(self):
        return [x for x in np.unique(np.array(self.sectors_.values()))]

    def get_companies(self):
        return [x for x in self.sectors_.keys()]
