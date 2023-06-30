import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
from google.cloud.storage import Client
from io import StringIO
import os


#engine = db.create_engine("mysql://root:root@192.168.1.12:3310/retail_db")
#conn = engine.connect()

#customers_df = pd.read_sql_query(text('SELECT * FROM customers'), con=conn)

class Extract():
    def __init__(self) -> None:
        self.process = 'Extractprocess'

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/user/app/ProyectoEndToEndPython/Proyecto/credenciales/eternal-ship-383701-df49bf9dc5a9.json"
    def read_cloud_storage (self, bucketName, fileName):
        
        client= Client()
        bucket = client.get_bucket(bucketName)
        blob= bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))
        return df
        
    def read_mysql(self,table_name,db_name):
        engine = db.create_engine(f"mysql://root:root@192.168.1.3:3310/{db_name}")
        conn = engine.connect()
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        return df
