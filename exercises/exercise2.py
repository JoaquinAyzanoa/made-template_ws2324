import pandas as pd
import sqlalchemy as sql

class pipeline():

    def __init__(self, url, db_name, table_name):
        #save url link
        self._url = url
        self._db_name = db_name
        self._table_name = table_name
        self._df = None

    def get_data(self):
        #read the url an save it in a dataframe
        self._df = pd.read_csv(self._url, delimiter=';')
            
    def drop_status(self):
        # drop Status column
        self._df.drop('Status', axis=1, inplace=True)

    def drop_empty_values(self):
        # Empty cells are considered invalid
        self._df.dropna(subset=self._df.columns.to_list(), inplace=True)
    
    def drop_invalid_values(self):
        # Valid "Verkehr" values are "FV", "RV", "nur DPN"
        Verkehr_list = ["FV", "RV", "nur DPN"]
        self._df = self._df[self._df['Verkehr'].isin(Verkehr_list)]
        # Valid "Laenge", "Breite" values are geographic coordinate system values between and including -90 and 90
        # Change type str -> float. I had to chance ',' for '.'
        self._df['Laenge'] = self._df['Laenge'].str.replace(',', '.').astype(float)
        self._df['Breite'] = self._df['Breite'].str.replace(',', '.').astype(float)
        # Make the comparison
        self._df = self._df[self._df['Laenge'].between(-90., 90.) & self._df['Breite'].between(-90., 90.)]
        #Valid "IFOPT" values
        pattern = r'^.{2}:\d+:\d+(?::\d+)?$'
        # ^.{2} any two characters at the beginning.
        # :\d+:\d+ colon and one or more digits, another colon, and one or more digits.
        # (?::\d+)?$ optionally followed by another colon and one or more digits
        self._df = self._df[self._df['IFOPT'].str.contains(pattern)]

    def create_sqldb(self):
        engine = sql.create_engine(f"sqlite:///{self._db_name}.sqlite", echo=False)
        dtype_mapping = {
            'EVA_NR': sql.types.BIGINT, 
            'DS100': sql.types.TEXT, 
            'IFOPT': sql.types.TEXT, 
            'NAME': sql.types.TEXT, 
            'Verkehr': sql.types.TEXT,
            'Laenge': sql.types.FLOAT, 
            'Breite': sql.types.FLOAT, 
            'Betreiber_Name': sql.types.TEXT,
            'Betreiber_Nr': sql.types.BIGINT, 
            }
        self._df.to_sql(self._table_name, con=engine, if_exists="replace", index=False, dtype=dtype_mapping)


    def run(self):
        self.get_data()
        self.drop_status()
        self.drop_empty_values()
        self.drop_invalid_values()
        self.create_sqldb()
        return self._df.copy()


if __name__ == '__main__':
    db_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    db_name = 'trainstops'
    table_name = 'trainstops'
    pp = pipeline(db_url, db_name, table_name)
    df = pp.run()
