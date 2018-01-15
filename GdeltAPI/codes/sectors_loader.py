import pandas as pd


class SectorsLoader:
    sectors_ = {}

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