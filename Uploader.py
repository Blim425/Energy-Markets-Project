import os
import pyodbc
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.types import Date, Integer, String, Numeric, VARCHAR
########################################################################################################################
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER={DESKTOP-PKB9LQ6};'
    'DATABASE={BryceTest};'
    'Trusted_Connection=yes;')
conn = pyodbc.connect(connection_string)
engine = sa.create_engine('mssql+pyodbc://', creator=lambda:conn)
cursor = conn.cursor()

########################################################################################################################
## Create Final table, Staging Table, DateChecker to programattically combine new CSV into database
inspector = sa.inspect(engine)
if not inspector.has_table('FinalEnergyPrices'):
    metadata = sa.MetaData()
    sa.Table(
        'FinalEnergyPrices',
        metadata,
        sa.Column('TradingDate', Date),
        sa.Column('TradingPeriod', Integer),
        sa.Column('PointOfConnection', VARCHAR(50)),
        sa.Column('DollarsPerMegawattHour', Numeric(10, 2))
    )
    metadata.create_all(engine)
if not inspector.has_table('StagingEnergyPrices'):
    metadata = sa.MetaData()
    sa.Table(
        'StagingEnergyPrices',
        metadata,
        sa.Column('TradingDate', Date),
        sa.Column('TradingPeriod', Integer),
        sa.Column('PointOfConnection', VARCHAR(50)),
        sa.Column('DollarsPerMegawattHour', Numeric(10, 2))
    )
    metadata.create_all(engine)
if not inspector.has_table('DateChecker'):
    metadata = sa.MetaData()
    sa.Table(
        'DateChecker',
        metadata,
        sa.Column('UniqueTradingDates', Date)
    )
    metadata.create_all(engine)
########################################################################################################################
def uploader_main(Type):

    if Type == "Daily":
        # Daily version
        files_path = "C:/Users/bryce/OneDrive - The University of Auckland/Electricity data"
    if Type == "Monthly":
        # Monthly Version
        files_path = "C:/Users/bryce/OneDrive/Desktop/Personal Projects Desktop/Electricity Data Monthly"
    # initialise file list
    file_list = [f for f in os.listdir(files_path) if f.endswith(".csv")]

    ## Go through each file in the file list and add it onto the database

    # initialise counter to print progress
    counter = 0

    for file in file_list:
        counter += 1
        file_path = '/'.join([files_path, file])

        # Bulk Insert into the staging table
        with engine.begin() as conn:
            bulk_insert_sql = (f"BULK INSERT [StagingEnergyPrices] FROM '{file_path}' WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',',ROWTERMINATOR = '0x0a')")
            conn.execute(sa.text(bulk_insert_sql))

        # Transfer Staging to Final when dates dont overlap - string
        insert_sql = '''
        INSERT INTO FinalEnergyPrices (TradingDate, TradingPeriod, PointOfConnection, DollarsPerMegawattHour)
        SELECT s.TradingDate, s.TradingPeriod, s.PointOfConnection, s.DollarsPerMegawattHour
        FROM StagingEnergyPrices s
        LEFT JOIN DateChecker ON s.TradingDate = DateChecker.UniqueTradingDates
        WHERE DateChecker.UniqueTradingDates IS NULL
        '''
        with engine.begin() as conn:
            # Add Staging into Final with INSERT statement
            conn.execute(sa.text(insert_sql))

        with engine.begin() as conn:
            # Upload the unique dates in Staging file (Which ultimately becomes the unique dates in Final)
            Insert_Unique_dates = '''
            INSERT INTO DateChecker(UniqueTradingDates)
            SELECT DISTINCT TradingDate 
            FROM StagingEnergyPrices
            LEFT JOIN DateChecker ON StagingEnergyPrices.TradingDate = DateChecker.UniqueTradingDates
            WHERE DateChecker.UniqueTradingDates IS NULL
            '''
            conn.execute(sa.text(Insert_Unique_dates))


            # IMPORTANT: Clear the staging table
            delete_Staging_sql = (f"DELETE FROM [StagingEnergyPrices]")
            conn.execute(sa.text(delete_Staging_sql))
            print('File Mergeing No: {:5d}    File:{:>34}'.format(counter, file))

    print("Finished")

    conn.close()

if __name__ == "__main__":
    # uploader_main("Monthly")
    uploader_main("Daily")