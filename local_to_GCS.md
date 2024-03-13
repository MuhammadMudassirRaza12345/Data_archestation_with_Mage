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
        """
        Template for loading data from API
        """
        url_yellow_taxi = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
        # url_green_taxi = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'

    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }

    # parse_dates_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    parse_dates_yellow_taxi = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    return pd.read_csv(url_yellow_taxi, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates_yellow_taxi)


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
      def transform(data, *args, **kwargs):
      
          print(data['passenger_count'].isin([0]).sum())
          non_zero_passengers_df = data[data['passenger_count'] > 0]
      
          return non_zero_passengers_df
          
          # zero_passengers_df = data[data['passenger_count'].isin([0]).sum()]
      
          # return data


      # @test
      def test_output(output, *args) -> None:
          """
          Template code for testing the output of the block.
          """
          assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'

# Data Exporter -- > python --> GCS

         from mage_ai.settings.repo import get_repo_path
        from mage_ai.io.config import ConfigFileLoader
        from mage_ai.io.google_cloud_storage import GoogleCloudStorage
        from pandas import DataFrame
        from os import path
        
        if 'data_exporter' not in globals():
            from mage_ai.data_preparation.decorators import data_exporter


        @data_exporter
        def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
            """
            Template for exporting data to a Google Cloud Storage bucket.
            Specify your configuration settings in 'io_config.yaml'.
        
            Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
            """
            now=kwargs.get('execution_date')
            now_fpath =now.strftime("%Y/%m%d")
            config_path = path.join(get_repo_path(), 'io_config.yaml')
            config_profile = 'default'
        
            bucket_name = 'mage_learn'
            object_key = f'{now_fpath}/daily_trips.parquet'
        
            GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
                df,
                bucket_name,
                object_key,
            )



# Data Exporter -- > python --> GCS   using pyarrow 

      import pyarrow as pa
      import pyarrow.parquet as pq
      from pandas import DataFrame
      import os
      
      if 'data_exporter' not in globals():
          from mage_ai.data_preparation.decorators import data_exporter
      
      # update the variables below
      os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/credentials/your_creditials.json'
      project_id = 'your project id'
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
![Local to gcs ](https://github.com/MuhammadMudassirRaza12345/Data_archestation_with_Mage/blob/main/local_to_GCS.png)
