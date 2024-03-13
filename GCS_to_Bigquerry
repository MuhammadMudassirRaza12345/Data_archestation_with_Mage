# Data Loader --> python --> Google Cloud Storage

        from mage_ai.settings.repo import get_repo_path
        from mage_ai.io.config import ConfigFileLoader
        from mage_ai.io.google_cloud_storage import GoogleCloudStorage
        from os import path
        if 'data_loader' not in globals():
            from mage_ai.data_preparation.decorators import data_loader
        if 'test' not in globals():
            from mage_ai.data_preparation.decorators import test


        @data_loader
        def load_from_google_cloud_storage(*args, **kwargs):
            """
            Template for loading data from a Google Cloud Storage bucket.
            Specify your configuration settings in 'io_config.yaml'.

            Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
            """
            config_path = path.join(get_repo_path(), 'io_config.yaml')
            config_profile = 'default'
        
            bucket_name = 'mage_learn'
            object_key = 'nyc_taxi_data.parquet'
        
            return GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).load(
                bucket_name,
                object_key,
            )


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
            data.columns= (data.columns
                           .str.replace(' ','_')
                           .str.lower()
                        )
                         
        return data


      # @test
      # def test_output(output, *args) -> None:
      #     """
      #     Template code for testing the output of the block.
      #     """
      #     assert output is not None, 'The output is undefined'

# Data Exporter -- > python -->  Big Querry

      from mage_ai.settings.repo import get_repo_path
      from mage_ai.io.bigquery import BigQuery
      from mage_ai.io.config import ConfigFileLoader
      from pandas import DataFrame
      from os import path

      if 'data_exporter' not in globals():
          from mage_ai.data_preparation.decorators import data_exporter


      @data_exporter
      def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
          """
          Template for exporting data to a BigQuery warehouse.
          Specify your configuration settings in 'io_config.yaml'.

      Docs: https://docs.mage.ai/design/data-loading#bigquery
      """
      table_id = 'my-learning-gcp-123.ny_taxi.yellow_cab_data'
      config_path = path.join(get_repo_path(), 'io_config.yaml')
      config_profile = 'default'
  
      BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
          df,
          table_id,
          if_exists='replace',  # Specify resolution policy if table name already exists
      )

# Pipeline view 
![gcs to Big querry ](https://github.com/MuhammadMudassirRaza12345/Data_archestation_with_Mage/blob/main/gcs_to_big_querry%20.png)
