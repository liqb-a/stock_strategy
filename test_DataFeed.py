import unittest
from DataFeed import UniversalDataFeed
import pandas as pd

class TestUniversalDataFeed(unittest.TestCase):

    def test_fetch_data(self):
        symbol = '300493'
        start_date = '20250101'
        end_date = '20250214'
        
        df = UniversalDataFeed.fetch_data(symbol, start_date, end_date)

        #print df to check the data
        print(df)
        
        # Check if the dataframe is not empty
        self.assertFalse(df.empty, "The dataframe is empty")
        
        # Check if the dataframe has the correct columns
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        self.assertTrue(all(column in df.columns for column in expected_columns), "Dataframe does not have the expected columns")
        
        # Check if the index is of datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df.index), "Index is not of datetime type")
        
        # Check if the data is within the specified date range
        self.assertTrue(df.index.min() >= pd.to_datetime(start_date), "Data contains dates before the start date")
        self.assertTrue(df.index.max() <= pd.to_datetime(end_date), "Data contains dates after the end date")

if __name__ == '__main__':
    unittest.main()