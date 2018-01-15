import pandas as pd


class SectorsLoader:
    def load_company_sectors(self):
        sectors_df = pd.read_csv('company_codes.csv')
        result = {}
        for row_index in range(len(sectors_df)):
            company_name = sectors_df.iloc[row_index].Actor1Name
            sector = sectors_df.iloc[row_index].Sector
            result[company_name] = sector
        return result
