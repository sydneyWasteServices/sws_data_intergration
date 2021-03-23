import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, Date, Time, SmallInteger, Text
import os


# root_path = '/media/sf_ubuntuShareDrive/Datasets/booking_weekly'

# for filename in os.listdir(root_path):
#     if filename.endswith(".csv"):
#         file_path = f"{root_path}/{filename}"
#         insert_booking_df_to_stage_table_db_1(file_path)

print("Files ready to insert into DB")
print()

root_path = '/media/sf_ubuntuShareDrive/Datasets/booking_weekly'

for filename in os.listdir(root_path):
    print(filename)

def insert_booking_df_to_stage_table_db_1(path):
    print()
    print(path)
#     encoding='iso8859' encoding='cp1252'


    df = (
            pd
             .read_csv(
                       path,
                       dtype={"Schd Time Start" : str, "PO": str, "Route number" : str},
                       encoding='iso8859')
    )
    
    df["Date"] = pd.to_datetime(df["Date"], format='%d/%m/%y')
    df[['Route number', 'Weekday']] = df['Route number'].str.split('-', 1, expand=True)
    
    db = "STAGE_TABLES_DB_1"
    
    engine_str = f"mssql+pyodbc://SA:ploi?H8597@gordonswsmaster/{db}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(engine_str)
    df.to_sql(name="STAGE_TABLE_1", con=engine, schema="BOOKING", if_exists="replace", method='multi' ,dtype={
        'Job No': Float(precision=5, asdecimal=True),
        'Date': Date,
        'Schd Time Start': String(length=350),
        'Schd Time End': String(length=350),
        'Latitude': Float(precision=7, asdecimal=True),
        'Longitude': Float(precision=7, asdecimal=True),
        'Customer number': Float(precision=4),
        'Customer Name': String(length=800),
        'Site Name': String(length=800),
        'Address 1': String(length=1200),
        'Address 2': String(length=1200),
        'City': String(length=500),
        'State': String(length=30),
        'PostCode': Integer(),
        'Zone': String(length=500),
        'Phone': String(length=600),
        'Qty Scheduled': SmallInteger(),
        'Qty Serviced': SmallInteger(),
        'Serv Type': String(length=600),
        'Container Type': String(length=20),
        'Bin Volume': Float(precision=5),
        'Status': String(length=5),
        'Truck number': String(length=50),
        'Route number': String(length=50),
        'Generate ID': String(length=500),
        'Initial Entry Date': String(length=350),
        'Weight': Float(precision=5),
        'Prorated Weight': Float(precision=5),
        'Booking Reference 1': String(length=200),
        'Booking Reference 2': String(length=200),
        'Alternate Ref No 1': String(length=200),
        'Alternate Ref No 2': String(length=200),
        'Alternate Service Ref 1': String(length=200),
        'Alternate Service Ref 2': String(length=200),
        'Notes': Text(length=8000),
        'Directions': Text(length=8000),
        'CheckLists': String(length=300),
        'Waste Type': String(length=350),
        'Tip Site': String(length=450),
        'Price': Integer(),
        'PO': String(length=200)
    })
    

for filename in os.listdir(root_path):
    if filename.endswith(".csv"):
        file_path = f"{root_path}/{filename}"
        insert_booking_df_to_stage_table_db_1(file_path)