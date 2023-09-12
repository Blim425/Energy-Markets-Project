import os
import pyodbc
import pandas as pd
import sqlalchemy as sa
import csv
from sqlalchemy.types import Date, Integer, String, Numeric, VARCHAR

########################################################################################################################
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER={DESKTOP-PKB9LQ6};'
    'DATABASE={BryceTest};'
    'Trusted_Connection=yes;')
conn = pyodbc.connect(connection_string)
engine = sa.create_engine('mssql+pyodbc://', creator=lambda:conn)

########################################################################################################################
if __name__ == '__main__':
    ####CHANGE HERE####
    OTABEN = False
    ALLNODES = True

    if OTABEN:
        # Set start End Dates
        Start_Date = '2010-01-01'
        End_Date = '2024-01-01'
        # Get the POCs
        POC_List = ['BEN2201', 'OTA0221']

        # Query
        with engine.begin() as conn:
            # For now let us only take Otahuhu and Benmore
            sql_query = f'''
            SELECT *
            FROM FinalEnergyPrices
            WHERE TradingDate BETWEEN '{Start_Date}' AND '{End_Date}'
            AND PointOfConnection IN ({', '.join(["'"+item+"'" for item in POC_List])})'''
            print("Querying...")
            results = conn.execute(sa.text(sql_query)).fetchall()
            print("Finished Query")

        # Get the column names
        inspector = sa.inspect(engine)
        columns = inspector.get_columns('FinalEnergyPrices')
        column_names = [column['name'] for column in columns]

        # filepath
        output_file = 'C:/Users/bryce/OneDrive - The University of Auckland/PersonalProjects/Query outputs/OTABEN_PRICES.csv'

        # Write the results to CSV
        with open(output_file, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the column headers using inspector
            csvwriter.writerow(column_names)
            # Write the data rows
            csvwriter.writerows(results)

    if ALLNODES:
        # Set start End Dates
        Start_Date = '2022-01-01'
        End_Date = '2024-01-01'

        # Query
        with engine.begin() as conn:
            # For now let us only take Otahuhu and Benmore
            sql_query = f'''
            SELECT *
            FROM FinalEnergyPrices
            WHERE TradingDate BETWEEN '{Start_Date}' AND '{End_Date}'
            '''
            print("Querying...")
            results = conn.execute(sa.text(sql_query)).fetchall()
            print("Finished Query")

        # Get the column names
        inspector = sa.inspect(engine)
        columns = inspector.get_columns('FinalEnergyPrices')
        column_names = [column['name'] for column in columns]

        # filepath
        output_file = 'C:/Users/bryce/OneDrive - The University of Auckland/PersonalProjects/Query outputs/2022-2023AllNodes.csv'

        # Write the results to CSV
        with open(output_file, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the column headers using inspector
            csvwriter.writerow(column_names)
            # Write the data rows
            csvwriter.writerows(results)