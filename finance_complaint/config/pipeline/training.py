import os, sys

from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from finance_complaint.constant.training_pipeline_config import *
from finance_complaint.constant import *
from finance_complaint.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig, DataValidationConfig, DataTransformationConfig
from finance_complaint.constant.training_pipeline_config.data_ingestion import *
from finance_complaint.constant.training_pipeline_config.data_validation import *
from finance_complaint.constant.training_pipeline_config.data_transformation import *
from finance_complaint.entity.metadata_info import DataIngestionMetadata


class FinanceConfig:
    """
    Class Name: FinanceConfig
    Description: Configuration class for the finance project.
    Author Name: prashantmalge181@gmail.com
    """
    def __init__(self, pipeline_name: str = PIPELINE_NAME, timestamp=TIMESTAMP):
        """
        Initializes a FinanceConfig instance.

        Args:
            pipeline_name (str): Name of the pipeline. Defaults to PIPELINE_NAME.
            timestamp (str): Current timestamp. Defaults to TIMESTAMP.
        """
        try:
            # Set the pipeline_name and timestamp attributes
            self.pipeline_name = pipeline_name
            self.timestamp = timestamp

            # Get the pipeline configuration
            self.pipeline_config = self.get_pipeline_config()

        except Exception as e:
            raise FinanceException(e, sys)

        

    def get_pipeline_config(self) -> TrainingPipelineConfig:
        """
        Method Name: get_pipeline_config
        Description: Get the pipeline configuration.
        Returns: The TrainingPipelineConfig object.
        On Failure: Raises a FinanceException if there is an error.
        """
        try:
            # Set the artifact directory
            artifact_dir = PIPELINE_ARTIFACT_DIR

            # Create the TrainingPipelineConfig object
            pipeline_config = TrainingPipelineConfig(
                pipeline_name=self.pipeline_name,
                artifact_dir=artifact_dir
            )

            logger.info(f"Pipeline Config: {pipeline_config}")
            return pipeline_config

        except Exception as e:
            raise FinanceException(e, sys)

        

    def get_data_ingestion_config(self, from_date=DATA_INGESTION_MIN_START_DATE, to_date=None) -> DataIngestionConfig:
        """
        Method Name: get_data_ingestion_config
        Description: Get the data ingestion configuration.
        Inputs:
            - from_date: Start date of data ingestion.
            - to_date: End date of data ingestion.
        Returns: The DataIngestionConfig object.
        On Failure: Raises a FinanceException if there is an error.
        """
        try:
            # Parse minimum start date
            min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d")

            # Parse from_date
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            if from_date_obj < min_start_date:
                from_date = DATA_INGESTION_MIN_START_DATE

            # Set default to_date if not provided
            if to_date is None:
                to_date = datetime.now().strftime("%Y-%m-%d")

            # Create data ingestion directories
            data_ingestion_master_dir = os.path.join(self.pipeline_config.artifact_dir, DATA_INGESTION_DIR)
            os.makedirs(data_ingestion_master_dir, exist_ok=True)
            data_ingestion_dir = os.path.join(data_ingestion_master_dir, self.timestamp)
            os.makedirs(data_ingestion_dir, exist_ok=True)

            # Set metadata file path
            metadata_file_path = os.path.join(data_ingestion_master_dir, DATA_INGESTION_METADATA_FILE_NAME)

            # Create DataIngestionMetadata object
            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=metadata_file_path)

            # If metadata is available, get metadata information
            if data_ingestion_metadata.is_meta_data_available:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                from_date = metadata_info.to_date

            # Create download directory
            download_dir = os.path.join(data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR)
            os.makedirs(download_dir, exist_ok=True)

            # Create failed directory
            failed_dir = os.path.join(data_ingestion_dir, DATA_INGESTION_FAILED_DIR)
            os.makedirs(failed_dir, exist_ok=True)

            # Create feature store directory
            feature_store_dir = os.path.join(data_ingestion_master_dir, DATA_INGESTION_FEATURE_STORE_DIR)
            os.makedirs(feature_store_dir, exist_ok=True)

            # Create DataIngestionConfig object
            data_ingestion_config = DataIngestionConfig(from_date=from_date,
                                                        to_date=to_date,
                                                        data_ingestion_dir=data_ingestion_dir,
                                                        download_dir=download_dir,
                                                        file_name=DATA_INGESTION_METADATA_FILE_NAME,
                                                        feature_store_dir=feature_store_dir,
                                                        failed_dir=failed_dir,
                                                        metadata_file_path=metadata_file_path,
                                                        datasource_url=DATA_INGESTION_DATA_SOURCE_URL)

            logger.info(f"Data ingestion config: {data_ingestion_config}")
            return data_ingestion_config

        except Exception as e:
            raise FinanceException(e, sys)





    def get_data_validation_config(self) -> DataValidationConfig:
        """
        Method Name: get_data_validation_config
        Description: Get the data validation configuration.
        Returns:
            DataValidationConfig: The DataValidationConfig object.
        Raises:
            FinanceException: If there is an error.
        """
        try:
            data_validation_dir = os.path.join(self.pipeline_config.artifact_dir, DATA_VALIDATION_DIR, self.timestamp)
            accepted_data_dir = os.path.join(data_validation_dir, DATA_VALIDATION_ACCEPTED_DATA_DIR)
            rejected_data_dir = os.path.join(data_validation_dir, DATA_VALIDATION_REJECTED_DATA_DIR)

            data_validation_config = DataValidationConfig(
                accepted_data_dir=accepted_data_dir,
                rejected_data_dir=rejected_data_dir,
                file_name=DATA_VALIDATION_FILE_NAME
            )

            logger.info(f"Data validation config: {data_validation_config}")

            return data_validation_config

        except Exception as e:
            raise FinanceException(e, sys)

        

    def get_data_transformation_config(self) -> DataTransformationConfig:
        """
        Method Name: get_data_transformation_config
        Description: Get the data transformation configuration.
        Returns:
            DataTransformationConfig: The DataTransformationConfig object.
        Raises:
            FinanceException: If there is an error.
        """
        try:
            data_transformation_dir = os.path.join(self.pipeline_config.artifact_dir, DATA_TRANSFORMATION_DIR)

            export_pipeline_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_PIPELINE_DIR)

            transformed_train_data_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_TRAIN_DIR)

            transformed_test_data_dir = os.path.join(data_transformation_dir, DATA_TRANSFORMATION_TEST_DIR)

            data_transformation_config = DataTransformationConfig(
                export_pipeline_dir=export_pipeline_dir,
                transformed_test_dir=transformed_test_data_dir,
                transformed_train_dir=transformed_train_data_dir,
                file_name=DATA_TRANSFORMATION_FILE_NAME,
                test_size=DATA_TRANSFORMATION_TEST_SIZE
            )
            
            logger.info(f"Data transformation config: {data_transformation_config}")

            return data_transformation_config
        except Exception as e:
            raise FinanceException(e, sys)

        
        
# if __name__ == '__main__':
#     finance = FinanceConfig()
#     print(finance.get_data_transformation_config())