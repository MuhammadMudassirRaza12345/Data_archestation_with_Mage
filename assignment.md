# Data Loader --> python --> API

      import io
      import pandas as pd
      import requests
      if 'data_loader' not in globals():
          from mage_ai.data_preparation.decorators import data_loader
      if 'test' not in globals():
          from mage_ai.data_preparation.decorators import test


      @data_loader
      def load_data_from_api(*args, **kwargs):
          file_paths = [
          'green_tripdata_2020-10.csv.gz',
          'green_tripdata_2020-11.csv.gz',
          'green_tripdata_2020-12.csv.gz',
          ]

      dfs = []

      for file_path in file_paths:
          # If dealing with URLs, uncomment the following lines to download the file content
          # response = requests.get(file_path)
          # file_content = io.BytesIO(response.content)
          # df = pd.read_csv(file_content, compression='gzip', header=0, sep=',', quotechar='"')
          
          # For local files, simply read them
          df = pd.read_csv(file_path, compression='gzip', header=0, sep=',', quotechar='"')
          
          dfs.append(df)

      return pd.concat(dfs, ignore_index=True)


      @test
      def test_output(output, *args) -> None:
          """
          Template code for testing the output of the block.
          """
          assert output is not None, 'The output is undefined'


# Transformer --> Python --> Generic(Template)

        if 'transformer' not in globals():
        from mage_ai.data_preparation.decorators import transformer
        if 'test' not in globals():
            from mage_ai.data_preparation.decorators import test


        @transformer
        def transform(df, *args, **kwargs):
            df = df[(df['passenger_count'] > 0) & (df['trip_distance'] > 0)]
         
            df['lpep_pickup_date'] = pd.to_datetime(df['lpep_pickup_datetime']).dt.date
        
         
            df.columns = [col.lower().replace(" ", "_") for col in df.columns] 
     

        return df


        @test
        def test_output(output, *args) -> None:
            """
            Template code for testing the output of the block.
            """
            assert output is not None, 'The output is undefined'


# Data Exporter -- > python --> postgres sql 

      from mage_ai.settings.repo import get_repo_path
      from mage_ai.io.config import ConfigFileLoader
      from mage_ai.io.postgres import Postgres
      from pandas import DataFrame
      from os import path
      
      if 'data_exporter' not in globals():
          from mage_ai.data_preparation.decorators import data_exporter


      @data_exporter
      def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
          """
          Template for exporting data to a PostgreSQL database.
          Specify your configuration settings in 'io_config.yaml'.
      
          Docs: https://docs.mage.ai/design/data-loading#postgresql
          """
          schema_name = 'mage'  # Specify the name of the schema to export data to
          table_name = 'green_taxi'  # Specify the name of the table to export data to
          config_path = path.join(get_repo_path(), 'io_config.yaml')
          config_profile = 'dev'

        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                df,
                schema_name,
                table_name,
                index=False,  # Specifies whether to include index in exported table
                if_exists='replace',  # Specify resolution policy if table name already exists
            )

# Data Exporter -- > python --> GCS 

      import pyarrow as pa
      import pyarrow.parquet as pq
      from pandas import DataFrame
      import os
      
      if 'data_exporter' not in globals():
          from mage_ai.data_preparation.decorators import data_exporter
      
      # update the variables below
      os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/credentials/7b6eff062c19e5c7eb6bc6aef08b8b2f8922406b.json'
      project_id = 'my-learning-gcp-123'
      bucket_name = 'mage_learn'
      object_key = 'green_taxi_data.parquet'
      table_name = 'ny_taxi_data'
      root_path = f'{bucket_name}/{table_name}'
      
      
      @data_exporter
      def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
          # creating a new date column from the existing datetime column
          df['tpep_pickup_date'] = df['tpep_pickup_datetime'].dt.date
      
          table = pa.Table.from_pandas(df)
      
          gcs = pa.fs.GcsFileSystem()
      
          pq.write_to_dataset(
              table,
              root_path=root_path,
              partition_cols=['tpep_pickup_date'],
              filesystem=gcs
          )

# Pipeline view 
![Assignment ](https://github.com/MuhammadMudassirRaza12345/Data_archestation_with_Mage/blob/main/assignment%20.png)
