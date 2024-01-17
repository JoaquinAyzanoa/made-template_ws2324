import pandas as pd
import urllib.request
import zipfile
import os
import sqlalchemy as sql

class DataPipeline:
    def __init__(self, url, db_name, table_name):
        #save url link
        self._url = url
        self._zip_file_path = None
        self._db_name = db_name
        self._table_name = table_name
        self._df = None

    def download_zip(self):
        # extract zip file and save as csvfile
        self._zip_file_path = 'mowesta.zip'
        urllib.request.urlretrieve(self._url, self._zip_file_path)

    def unzip(self, filename):
        # Extract the CSV file from the ZIP
        with zipfile.ZipFile(self._zip_file_path, 'r') as zr:
            # CSV file is 'data.csv'
            extracted_csv = zr.extract(filename)
        # clean data
        os.remove(self._zip_file_path)
        return extracted_csv
    

    def get_dataframe(self, extracted_csv, filename):
        # Read the extracted CSV file into a Pandas DataFrame
        self._df = pd.read_csv(extracted_csv, delimiter=";", decimal=',', usecols=range(11), header=None)
        self._df.columns = self._df.iloc[0] #Replace column names
        self._df.drop(index=0, inplace=True) #Erase row 0 with column names
        # clean data
        os.remove(filename)
        return self._df
    
    def reshape_df(self):
        # select columns
        selected_columns = ['Geraet', 'Hersteller', 'Model', 'Monat',
            'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv']
        self._df = self._df[selected_columns]
        
        # Rename columns
        column_rename = {
            'Temperatur in °C (DWD)': 'Temperatur',
            'Batterietemperatur in °C': 'Batterietemperatur'
        }
        self._df = self._df.rename(columns=column_rename)
        
        # Discard all columns to the right of “​​Geraet aktiv”
        self._df = self._df.loc[:, :'Geraet aktiv']
        
        return self._df
    
    def transform_df(self):
        # transform column value from celsius to farenheit
        self._df['Temperatur'] = self._df['Temperatur'].str.replace(',','.').astype(float)
        self._df['Batterietemperatur'] = self._df['Batterietemperatur'].str.replace(',','.').astype(float)
        self._df['Temperatur'] = self._df['Temperatur'] * 9/5 + 32
        self._df['Batterietemperatur'] = self._df['Batterietemperatur'] * 9/5 + 32
        return self._df
    
    def validate_date(self):
        # take only row with values more than 0
        self._df = self._df[self._df['Geraet'].apply(int) > 0]
        self._df = self._df[self._df['Monat'].apply(int) > 0]
        self._df = self._df[self._df['Monat'].apply(int) < 13]
        return self._df
    
    def create_sqldb(self):
        dtype_mapping = {
        'Geraet': sql.types.BIGINT,
        'Hersteller': sql.types.TEXT,
        'Model': sql.types.TEXT,
        'Monat': sql.types.BIGINT,
        'Temperatur': sql.types.FLOAT,
        'Batterietemperatur': sql.types.FLOAT,
        'Geraet aktiv': sql.types.TEXT}
        #Create the sqlite database
        engine = sql.create_engine(f"sqlite:///data/{self._db_name}.sqlite", echo=False)
        self._df.to_sql(self._table_name, con=engine, if_exists="replace", 
                        index=False, dtype = dtype_mapping)
        
        return self._df

    def run(self, filename):
        self.download_zip()
        extracted_csv = self.unzip(filename)
        self.get_dataframe(extracted_csv, filename)
        self.reshape_df()
        self.transform_df()
        self.validate_date()
        self.create_sqldb()
        return self._df


if __name__ == '__main__':
    
    url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
    dbname = 'temperatures'
    tbname = 'temperatures'
    filename = 'data.csv'
    dp = DataPipeline(url, dbname, tbname)
    df = dp.run(filename)
    print(df.columns)
    print(df['Geraet aktiv'].unique())
    