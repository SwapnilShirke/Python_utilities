import mysql.connector as connection
import pandas as pd
import os
from dotenv import load_dotenv



host = os.environ.get('HOST')
port = os.environ.get('PORT')
database= os.environ.get('DATABASE')
user =os.environ.get('USER')
password=os.environ.get('PASSWORD')


mydb = connection.connect(host=host, database = database,
user=user, password=password,port=port)
query = "SELECT * FROM table_name;"
df = pd.read_sql(query,mydb)
path=f"table_name.parquet"
df.to_parquet(path,
compression= 'gzip',
times="int96",
engine='fastparquet')

mydb.close()