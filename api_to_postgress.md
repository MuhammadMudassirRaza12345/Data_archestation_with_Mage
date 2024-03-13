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
                  schema_name = 'ny_taxi'  # Specify the name of the schema to export data to
                  table_name = 'yellow_taxi_data'  # Specify the name of the table to export data to
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

# check data load in postgres sql
![check data load in postgres sq ](https://github.com/MuhammadMudassirRaza12345/Data_archestation_with_Mage/blob/main/check_data_load_to_postgres_sql%20.png)

# Pipeline view 
![ Api to postgres  ](https://github.com/MuhammadMudassirRaza12345/Data_archestation_with_Mage/blob/main/api_to_postgres%20.png)
