import os, sys

from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from finance_complaint.constant.training_pipeline_config import *
from finance_complaint.constant import *
from finance_complaint.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig
from finance_complaint.constant.training_pipeline_config.data_ingestion import *


class FinanceConfig:
    def __init__(self,pipeline_name: str = PIPELINE_NAME, timestamp = TIMESTAMP):
        try:
            self.pipeline_name = pipeline_name
            self.timestamp = timestamp
            

        except Exception as e:
            raise FinanceException(e, sys)
        

    def get_pipeline_config(self) -> TrainingPipelineConfig:
        try:
            artifact_dir = PIPELINE_ARTIFACT_DIR
            pipeline_config = TrainingPipelineConfig(pipeline_name= self.pipeline_name,
                artifact_dir= artifact_dir
            )
            logger.info(f"Pipeline Config : {pipeline_config}")
            return pipeline_config
        
        except Exception as e:
            raise FinanceException(e, sys)
        
    def get_data_ingestion_config(self, from_date = DATA_INGESTION_MIN_START_DATE, to_date = None) -> DataIngestionConfig:
        try:
            
            min_start_date = datetime.strptime(DATA_INGESTION_MIN_START_DATE, "%Y-%m-%d")
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            if from_date_obj < min_start_date:
                from_date = DATA_INGESTION_MIN_START_DATE
            if to_date is None:
                to_date = datetime.now().strftime("%Y-%m-%d")

            

            data_ingestion_config = DataIngestionConfig(from_date = ,
                                                        to_date = , 
                                                        data_ingestion_dir = ,
                                                        download_dir = ,
                                                        file_name = , 
                                                        feature_store_dir = , 
                                                        failed_dir = , 
                                                        metadata_file_path = , 
                                                        datasource_url = , 
                                                        )

        except Exception as e:
            raise FinanceException(e, sys)

        from_date: str
        to_date: str
        data_ingestion_dir: str
        download_dir: str
        file_name: str
        feature_store_dir: str
        failed_dir: str
        metadata_file_path: str
        datasource_url: str
        

    