import os, sys

from pyspark.sql import DataFrame

from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.config_entity import DataTransformationConfig
from finance_complaint.entity.artifact_entity import DataValidationArtifact
from finance_complaint.config.pipeline.training import DataTransformation
from finance_complaint.component.training.data_validataion import DataValidation


class DataTransformation:
    def __init__(self, data_transformation_config : DataTransformationConfig, data_validation_artifact : DataValidationArtifact):

        try:
            self.data_transformation_config = data_transformation_config
            self.data_validation_artifact = data_validation_artifact


        except Exception as e:
            raise FinanceException(e, sys)
        
    @property
    def read_data(self) -> DataFrame:

        try:
            file_path : str = self.data_validation_artifacr.accepted_file_path

            dataframe = spark_session.read.parquet(file_path)

            dataframe.printSchema()

            return dataframe
        
        except Exception as e:
            raise FinanceException(e, sys)
        

    

if __name__ == "__main__":

    data_transformation_config = DataTransformation()
    artifact_validation = DataValidationArtifact()
    # data_validation_artifact = 
    # transormation = DataTransformation(data_transformation_config=, data_validation_artifact==)
