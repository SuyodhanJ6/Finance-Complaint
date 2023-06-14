import os, sys
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

from finance_complaint.config.spark_manager import spark_session

from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.component.training.data_ingestion import DataIngestion
from finance_complaint.entity.config_entity import DataValidatonConfig,MissingReport
from finance_complaint.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from finance_complaint.entity.schema import FinanceDataSchema

class DataValidation:
    """
    Class Name: DataValidation
    Description: Handles data validation process.
    Author Name: prashantmalge181@gmail.com
    Revision:
    """
    def __init__(self, data_validation_config: DataValidatonConfig, data_ingestion_artifact: DataIngestionArtifact,
                 schema=FinanceDataSchema()):
        """
        Initializes a DataValidation instance.

        Args:
            data_validation_config (DataValidationConfig): Configuration for data validation.
            data_ingestion_artifact (DataIngestionArtifact): Artifact for data ingestion.
            schema (FinanceDataSchema): Data schema for validation.

        Raises:
            FinanceException: If an error occurs during initialization.
        """
        try:
            logger.info("Starting data validation.")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema = schema
        except Exception as e:
            raise FinanceException(e, sys)



    def read_data(self) -> DataFrame:
        """
        Method Name: read_data
        Description: Reads the data from the feature store file and returns a DataFrame.
        Revision:
        Returns:
            DataFrame: The DataFrame containing the read data.

        Raises:
            FinanceException: If an error occurs during data reading.
        """
        try:
            dataframe = spark_session.read.parquet(self.data_ingestion_artifact.feature_store_file_path)
            logger.info(f"Data frame is created using file: {self.data_ingestion_artifact.feature_store_file_path}")
            logger.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")
            #dataframe, _ = dataframe.randomSplit([0.001, 0.999])
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

        
    
    def get_missing_report(self, dataframe: DataFrame) -> Dict[str, MissingReport]:
        """
        Method Name: get_missing_report
        Description: Generates missing reports for each column in the DataFrame.
        Revision:
        Args:
            dataframe (DataFrame): The DataFrame for which missing reports need to be generated.

        Returns:
            Dict[str, MissingReport]: A dictionary containing the missing report for each column.

        Raises:
            FinanceException: If an error occurs during the missing report generation.
        """
        try:
            missing_report: Dict[str, MissingReport] = dict()
            logger.info("Preparing missing reports for each column")
            number_of_row = dataframe.count()

            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(total_row=number_of_row,
                                                    missing_row=missing_row,
                                                    missing_percentage=missing_percentage
                                                    )
            logger.info(f"Missing report prepared: {missing_report}")
            return missing_report

        except Exception as e:
            raise FinanceException(e, sys)

        

    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float = 0.2) -> List[str]:
        """
        Method Name: get_unwanted_and_high_missing_value_columns
        Description: Retrieves unwanted columns with high missing values based on a threshold.
        Revision:
        Args:
            dataframe (DataFrame): The DataFrame for which unwanted columns need to be identified.
            threshold (float): The threshold value for missing values (default: 0.2).

        Returns:
            List[str]: A list of unwanted columns.

        Raises:
            FinanceException: If an error occurs during the identification of unwanted columns.
        """
        try:
            missing_report = self.get_missing_report(dataframe=dataframe)

            unwanted_column = self.schema.unwanted_columns

            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
                    logger.info(f"Missing report {column}: [{missing_report[column]}]")

            unwanted_column = list(set(unwanted_column))
            return unwanted_column

        except Exception as e:
            raise FinanceException(e, sys)



    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        """
        Method Name: drop_unwanted_columns
        Description: Drops unwanted columns from the DataFrame and writes them to a file.
        Revision:
        Args:
            dataframe (DataFrame): The DataFrame from which unwanted columns need to be dropped.

        Returns:
            DataFrame: The DataFrame after dropping unwanted columns.

        Raises:
            FinanceException: If an error occurs during the dropping of unwanted columns.
        """
        try:
            unwanted_columns: List[str] = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe)
            logger.info(f"Dropping columns: {','.join(unwanted_columns)}")

            dropped_dataframe: DataFrame = dataframe.drop(*unwanted_columns)

            # Create a new column indicating dropped columns with missing values
            dropped_dataframe = dropped_dataframe.withColumn("ERROR_MESSAGE", lit("Contains many missing values"))

            # Write dropped columns to a file
            rejected_dir = os.path.join(self.data_validation_config.rejected_data_dir, "missing_data")
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(rejected_dir, self.data_validation_config.file_name)
            logger.info(f"Writing dropped columns to file: {file_path}")
            dropped_dataframe.write.mode("append").parquet(file_path)

            logger.info(f"Remaining number of columns: {len(dropped_dataframe.columns)}")
            return dropped_dataframe

        except Exception as e:
            raise FinanceException(e, sys)



    def is_required_column_present(self, dataframe: DataFrame):
        """
        Method Name: is_required_column_present
        Description: Checks if all the required columns are present in the DataFrame.
        Revision:
        Args:
            dataframe (DataFrame): The DataFrame to be checked.

        Raises:
            FinanceException: If a required column is missing in the DataFrame.
        """
        try:
            columns = list(filter(lambda x: x in self.schema.required_columns, dataframe.columns))

            if len(columns) != len(self.schema.required_columns):
                raise Exception(f"Required column missing\n\
                    Expected columns: {self.schema.required_columns}\n\
                    Found columns: {columns}\
                    ")

        except Exception as e:
            raise FinanceException(e, sys)


    
    def get_unique_values_of_each_column(dataframe: DataFrame) -> None:
        """
        Method Name: get_unique_values_of_each_column
        Description: Calculates the number of unique values, missing values, and missing percentage for each column in the DataFrame.
        Revision:
        Args:
            dataframe (DataFrame): The DataFrame for which unique values and missing values need to be calculated.
        Returns:
            None
        Raises:
            FinanceException: If an error occurs during the calculation of unique values and missing values.

        """
        try:
            for column in dataframe.columns:
                n_unique: int = dataframe.select(col(column)).distinct().count()
                n_missing: int = dataframe.filter(col(column).isNull()).count()
                missing_percentage: float = (n_missing * 100) / dataframe.count()
                logger.info(f"Column: {column} contains {n_unique} value and missing perc: {missing_percentage} %.")
        except Exception as e:
            raise FinanceException(e, sys)

        

    def initialize_data_validation(self) -> DataValidationArtifact:
        """
        Method Name: initialize_data_validation
        Description: Initializes the data validation process by performing data preprocessing and saving the preprocessed data.
        Revision:
        Returns:
            DataValidationArtifact: The artifact containing the paths of the accepted and rejected data.

        Raises:
            FinanceException: If an error occurs during the data validation or saving of validating the  data.

        """
        try:
            logger.info(f"Initiating data preprocessing.")
            dataframe: DataFrame = self.read_data()

            logger.info(f"Dropping unwanted columns")
            dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            # validation to ensure that all required columns are available
            self.is_required_column_present(dataframe=dataframe)

            logger.info("Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(f"Expected Column: {self.schema.required_columns}\nPresent Columns: {dataframe.columns}")

            os.makedirs(self.data_validation_config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(self.data_validation_config.accepted_data_dir,
                                            self.data_validation_config.file_name
                                            )
            dataframe.write.parquet(accepted_file_path)

            artifact = DataValidationArtifact(accepted_file_path=accepted_file_path,
                                            rejected_dir=self.data_validation_config.rejected_data_dir
                                            )
            logger.info(f"Data validation artifact: [{artifact}]")

            return artifact

        except Exception as e:
            raise FinanceException(e, sys)


    

if __name__ == '__main__':
  
    config = FinanceConfig()
    data_ingestion_config = config.get_data_ingestion_config()
    data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config, n_try=5)
    data_ingestion_artifact = data_ingestion.initialize_data_ingestion()
    data_validation_config = config.get_data_validation_config()

    datavalidation = DataValidation(data_validation_config, data_ingestion_artifact)
    dataframe  = datavalidation.read_data()
    print(datavalidation.initialize_data_validation())