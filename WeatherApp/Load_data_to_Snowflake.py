import pandas as pd
import snowflake.connector
import os

# Load environment variables
SF_USER = os.environ.get('SF_USER')
SF_PASSWORD = os.environ.get('SF_PASSWORD')
SF_ACCOUNT = os.environ.get('SF_ACCOUNT')
SF_WAREHOUSE = os.environ.get('SF_WAREHOUSE')
SF_DATABASE = os.environ.get('WEATHER_DB')
SF_SCHEMA = os.environ.get('SF_SCHEMA')

# Load the CSV. In some processes we would create the dataset on the fly from other sources
df = pd.read_csv('./data/weather_dummy_data.csv')

# Connect to Snowflake DB
conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    warehouse=SF_WAREHOUSE,
    database=WEATHER_DB,
    schema=SF_SCHEMA
)

cursor = conn.cursor()

# Create table if not exists
cursor.execute(f"""
    CREATE OR REPLACE TABLE {SF_SCHEMA}.WEATHER (
        DATE DATE,
        CITY STRING,
        TEMP_C FLOAT,
        HUMIDITY INT
    )
""")

# Insert data
for _, row in df.iterrows():
    cursor.execute(
        f"INSERT INTO {SF_SCHEMA}.WEATHER VALUES (%s, %s, %s, %s)",
