# Importing necessary libraries for Google Drive authentication, data processing, and MySQL interaction
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import data_cleaning # Custom module for data cleaning

# Authenticating and initializing Google Drive
gauth = GoogleAuth()
gauth.LocalWebserverAuth() # Authenticating the user via local web server
drive = GoogleDrive(gauth) # Initializing the Google Drive instance with authentication

# Defining folder ID and local save path
folder_id = '1MVSC2XyRMcOgXYar38FYJY0Cs5bC0CwT'
os.makedirs("downloaded_csvs", exist_ok=True)

# Downloading CSV files from Google Drive
file_list = drive.ListFile({'q': f"'{folder_id}' in parents and mimeType='text/csv'"}).GetList()
dataframes = {}

for file in file_list:
    print(f'Downloading {file["title"]}...')
    file_path = f'downloaded_csvs/{file["title"]}'  # Defining local file path for download
    file.GetContentFile(file_path) # Downloading the file from Google Drive
    print(f'{file["title"]} downloaded successfully.')
    
    # Reading CSV into DataFrame
    delimiter = '\t' 
    dataframes[file["title"]] = pd.read_csv(file_path, delimiter=delimiter)

print("All files have been downloaded and loaded into DataFrames.")

# Loading environment variables for MySQL
load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
database = os.getenv("DB_NAME")

# Cleaning and preparing data with data_cleaning functions
df_products = data_cleaning.products(dataframes["Product.csv"])
df_sales = data_cleaning.sales(dataframes["Sales.csv"])
df_targets = data_cleaning.target(dataframes["Targets.csv"])

# Loading remaining data directly into Dataframes
df_region = dataframes["Region.csv"]
df_reseller = dataframes["Reseller.csv"]
df_salesPerson = dataframes["Salesperson.csv"]
df_salesPersonRegion = dataframes["SalespersonRegion.csv"]


# Establishing a connection to the MySql Database
connection = mysql.connector.connect(
        host = host,
        user= user,  
        password= password,  
        database= database  
    )

cursor = connection.cursor() # Initializing a cursor for database operations

# Inserting cleaned data into MySQL database as tables
tables = {
    'sales': df_sales,
    'sales_person_region': df_salesPersonRegion,
    'products': df_products,
    'targets': df_targets,
    'region': df_region,
    'reseller': df_reseller,
    'sales_person': df_salesPerson
}

# Creating SQLAlchemy engine for batch insertion into MySql Database
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Looping through the tables dictionary and inserting each Dataframe into its respective MySql table
for table_name, df in tables.items():
    df.to_sql(table_name, con=engine, if_exists='replace', index=False) # Replacing table if it already exists
    print(f"Data for '{table_name}' successfully exported to MySQL.")

# Closing the database connections after completion
engine.dispose()
print("All data successfully exported to MySQL.")
