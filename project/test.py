import unittest
import os
import pandas as pd
import pipeline
from pipeline import DataPipeline
import sqlalchemy as sql
import sqlite3

class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        # Set up test data
        self.test_csv_url = "https://www.landesdatenbank.nrw.de/ldbnrwws/downloader/00/tables/46251-02iz_00.csv"
        self.test_excel_url = "https://www.opengeodata.nrw.de/produkte/umwelt_klima/luftqualitaet/luqs/eu_jahreskenngroessen/LUQS-EU-Kenngroessen-2022.xlsx"
        self.db_name = "test_db"
        self.table_name = "test_table"
        #Set a dataframe for testing
        self.df_test = pd.DataFrame(
            {'Name': ['Bon', 'Anb', 'Ban', '---'],
             'Age': [45, 28, '-test-', 109],
             'Distance': [20.5, 21.5, 19.5, 'nulo'],
             'Code': ['-', 'nulo', 'AAA', 'AAAA']})


    def test_read_csv(self):
        #test if the output is a dataframe
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        lines_to_skip = list(range(1, 8))
        lines_to_read = 61
        df = dp.read_csv(lines_to_skip, lines_to_read)
        self.assertIsInstance(df, pd.DataFrame)

    def test_read_excel(self):
        #test if the output is a dataframe
        dp = DataPipeline(self.test_excel_url, self.db_name, self.table_name)
        sheet_name = 'EU-Jahreskenngrößen 2022'
        limits = (5, 158, 0, 17)
        df = dp.read_excel(sheet_name, limits)
        self.assertIsInstance(df, pd.DataFrame)

    def test_rename_cols(self):
        #Test if is changing the columns names
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        dp.set_df(self.df_test)
        cols_name = ['A', 'B', 'C', 'D']
        df = dp.rename_cols(cols_name)
        self.assertEqual(list(df.columns), cols_name)

    def test_del_missing_info(self):
        #Check if it is deleting missing info
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        dp.set_df(self.df_test)
        col_name = 'Code'
        character = '-'
        df = dp.del_missing_info(col_name, character)
        self.assertEqual(0, df[col_name].str.contains(character).sum())

    def test_replace_str(self):
        #Check if it is replacing str
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        dp.set_df(self.df_test)
        strings_to_replace = ['-test-', 'nulo']
        target = 'X'
        df = dp.replace_str(strings_to_replace, target)
        self.assertEqual(0, df.isin(strings_to_replace).sum().sum())

    def test_create_sqldb(self):
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        dp.set_df(pd.DataFrame(
            {'A': ['Bon', 'Anb', 'Ban'],
             'B': [45.1, 28.5, 109.5],
             'C': [20, 21, 19]}))
        dtype_mapping = {'A': sql.types.String, 
                         'B': sql.types.Float, 
                         'C': sql.types.Integer}
        db_dir = dp.create_sqldb(dtype_mapping)
        self.assertTrue(os.path.isfile(db_dir))
        

    def test_create_b1(self):
        #Check if it is creating correctly the database for vehicles.
        df, db_dir = pipeline.create_db_1()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertTrue(os.path.isfile(db_dir))

    def test_create_b2(self):
        #Check if it is creating correctly the database for airpollution.
        df, db_dir = pipeline.create_db_2()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertTrue(os.path.isfile(db_dir))

    def test_table_name(self):
        dp = DataPipeline(self.test_csv_url, self.db_name, self.table_name)
        dp.set_df(pd.DataFrame(
            {'A': ['Bon', 'Anb', 'Ban'],
             'B': [45.1, 28.5, 109.5],
             'C': [20, 21, 19]}))
        dtype_mapping = {'A': sql.types.String, 
                         'B': sql.types.Float, 
                         'C': sql.types.Integer}
        db_dir = dp.create_sqldb(dtype_mapping)

        # Verify that the expected table is present in the database
        with sqlite3.connect(db_dir) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [table_name[0] for table_name in cursor.fetchall()]
            self.assertIn(self.table_name, table_names)

    def test_table_name1(self):
        df, db_dir = pipeline.create_db_1()

        # Verify that the expected table is present in the database
        with sqlite3.connect(db_dir) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [table_name[0] for table_name in cursor.fetchall()]
            self.assertIn('vehicles', table_names)

    def test_table_name2(self):
        df, db_dir = pipeline.create_db_2()

        # Verify that the expected table is present in the database
        with sqlite3.connect(db_dir) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [table_name[0] for table_name in cursor.fetchall()]
            self.assertIn('airpollution', table_names)


    def test_dbtests_deleted(self):
        #Check if the databases created for testing are being deleted.
        if os.path.exists('./data/test_db.sqlite'):
            os.remove('./data/test_db.sqlite')

        if os.path.exists('./data/airpollution.sqlite'):
            os.remove('./data/airpollution.sqlite')

        if os.path.exists('./data/vehicles.sqlite'):
            os.remove('./data/vehicles.sqlite')

        self.assertFalse(os.path.isfile('./data/test_db.sqlite'))
        self.assertFalse(os.path.isfile('./data/airpollution.sqlite'))
        self.assertFalse(os.path.isfile('./data/vehicles.sqlite'))
    

if __name__ == '__main__':
    unittest.main(exit=False)

    


    
    
