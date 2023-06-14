import pandas as pd 
import os, sys
from datetime import datetime
import requests
import json
import re 
import uuid
import time 

from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from finance_complaint.entity.config_entity import DataIngestionConfig, DownloadUrl, DataIngestionArtifact
from finance_complaint.entity.metadata_info import DataIngestionMetadata
from finance_complaint.config.pipeline.training import FinanceConfig
from finance_complaint.config.spark_manager import spark_session

class DataIngestion:
    """
    Class Name: DataIngestion
    Description: Handles data ingestion process.
    Author Name: prashantmalge181@gmail.com
    Revision: 
    """
    def __init__(self, data_ingestion_config: DataIngestionConfig, n_try: int):
        """
        Initializes a DataIngestion instance.

        Args:
            data_ingestion_config (DataIngestionConfig): Configuration for data ingestion.
            n_try (int): Number of retry attempts for data ingestion.
        """
        try:
            logger.info(f"{'>>' * 20}Starting data ingestion.{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
            self.n_try = n_try
        except Exception as e:
            raise FinanceException(e, sys)


    def get_required_interval(self):
        """
        Method Name: get_required_interval
        Description: Calculates the required intervals based on the data ingestion configuration.
        Revision: 
        Returns: List[str]: List of intervals.
        On Failure: Raises FinanceException.

        """
        try:
            start_date = datetime.strptime(self.data_ingestion_config.from_date, '%Y-%m-%d')
            end_date = datetime.strptime(self.data_ingestion_config.to_date, '%Y-%m-%d')
            n_diff_days = (end_date - start_date).days

            freq_mapping = {
                365: "Y",  # Frequency is "Yearly" for more than 365 days difference
                30: "M",   # Frequency is "Monthly" for more than 30 days difference
                7: "W"     # Frequency is "Weekly" for more than 7 days difference
            }

            freq = None

            for days, frequency in freq_mapping.items():
                if n_diff_days >= days:
                    freq = frequency
                    break

            if freq is None:
                # Calculate intervals using two dates
                intervals = pd.date_range(start=self.data_ingestion_config.from_date, end=self.data_ingestion_config.to_date,
                                        periods=2).astype('str').to_list()

            else:
                # Calculate intervals with given frequency
                intervals = pd.date_range(start=self.data_ingestion_config.from_date, end=self.data_ingestion_config.to_date,
                                        freq=freq).astype('str').to_list()

            logger.info(f"Prepared Interval: {intervals}")
            return intervals

        except Exception as e:
            raise FinanceException(e, sys)


    def download_files(self):
        """
        Method Name: download_files
        Description: Downloads files based on the required intervals.
        Revision:
        Returns: None
        On Failure: Raises FinanceException.

        """
        try:
            get_intervals_list = self.get_required_interval()
            logger.info("Started downloading files")
            for index in range(1, len(get_intervals_list)):
                from_date, to_date = get_intervals_list[index-1], get_intervals_list[index]
                logger.debug(f"Generating data download url between {from_date} and {to_date}")
                datasource_url = self.data_ingestion_config.datasource_url
                url = datasource_url.replace("<todate>", to_date).replace("<fromdate>", from_date)            
                logger.debug(f"Url: {url}")
                file_name  = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.data_ingestion_config.download_dir, file_name)
                download_url = DownloadUrl(url=url, file_path=file_path, n_retry=self.n_try)
                self.download_data(download_url=download_url)
        except Exception as e:
            raise FinanceException(e, sys)

        
    def retry_download_data(self, data, download_url: DownloadUrl):
        """
        Method Name: retry_download_data
        Description: Retry the download process for a failed file.
        Revision:
        Args:
            data: Failed response.
            download_url (DownloadUrl): Download URL information.
        Returns: None
        On Failure: Raises FinanceException.

        """
        try:
            # Check if retry is still possible
            if download_url.n_retry == 0:
                self.failed_download_urls.append(download_url)
                logger.info(f"Unable to download file: {download_url.url}")
                return

            # Handle throttling by waiting for the specified time
            content = data.content.decode("utf-8")
            wait_seconds = re.findall(r'\d+', content)
            if wait_seconds:
                wait_time = int(wait_seconds[0]) + 2
                time.sleep(wait_time)

            # Save failed response for further analysis
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir, os.path.basename(download_url.file_path))
            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            # Retry the download by reducing the number of retry attempts
            updated_download_url = DownloadUrl(download_url.url, file_path=download_url.file_path, n_retry=download_url.n_retry - 1)
            self.download_data(download_url=updated_download_url)

        except Exception as e:
            raise FinanceException(e, sys)


    def download_data(self, download_url: DownloadUrl):
        """
        Method Name: download_data
        Description: Downloads data from the specified URL and saves it as a JSON file.
        Revision:
        Args:
            download_url (DownloadUrl): Download URL information.
        Returns: None
        On Failure: Raises FinanceException.

        """
        try:
            logger.info(f"Starting download operation: {download_url}")
            download_dir = os.path.dirname(download_url.file_path)
            # Creating download directory
            os.makedirs(download_dir, exist_ok=True)

            # Downloading data
            data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})
            try:
                logger.info(f"Started writing downloaded data into JSON file: {download_url.file_path}")
                # Saving download data into hard disk
                with open(download_url.file_path, 'w') as file_obj:
                    finance_complaint_data = list(map(lambda x: x["_source"],
                                                    filter(lambda x: "_source" in x.keys(),
                                                            json.loads(data.content)))
                                                )
                    json.dump(finance_complaint_data, file_obj)
                    logger.info(f"Downloaded data has been written into file: {download_url.file_path}")

            except Exception as e:
                logger.info("Failed to download, retrying...")
                # Removing file if it exists
                if os.path.exists(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data, download_url=download_url)
        except Exception as e:
            raise FinanceException(e, sys)

        
    def convert_files_to_parquet(self):
        """
        Method Name: convert_files_to_parquet
        Description: Converts JSON files to Parquet format and saves them to the specified directory.
        Revision:
        Returns: str: Path of the created Parquet file.
        On Failure: Raises FinanceException.

        """
        try:
            json_data_dir = self.data_ingestion_config.download_dir
            data_dir = self.data_ingestion_config.feature_store_dir
            output_file_name = self.data_ingestion_config.file_name
            os.makedirs(data_dir, exist_ok=True)

            file_path = os.path.join(data_dir, f"{output_file_name}")
            logger.info(f"Parquet file will be created at: {file_path}")
            if not os.path.exists(json_data_dir):
                return file_path

            for file_name in os.listdir(json_data_dir):
                json_file_path = os.path.join(json_data_dir, file_name)
                logger.debug(f"Converting {json_file_path} into Parquet format at {file_path}")
                df = spark_session.read.json(json_file_path)
                if df.count() > 0:
                    df.write.mode('append').parquet(file_path)
            return file_path
        except Exception as e:
            raise FinanceException(e, sys)


    def write_metadata(self, file_path: str) -> None:
        """
        Method Name: write_metadata
        Description: Writes metadata information to a metadata file to avoid redundant download and merging.
        Revision:
        Parameters:
            file_path (str): Path of the data file for which metadata is being written.
        Returns: None
        On Failure: Raises FinanceException.

        """
        try:
            logger.info("Writing metadata info into metadata file.")
            metadata_info = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_info.write_meta_info(from_date=self.data_ingestion_config.from_date,
                                        to_date=self.data_ingestion_config.to_date,
                                        data_file_path=file_path
                                        )
            logger.info("Metadata has been written.")
        except Exception as e:
            raise FinanceException(e, sys)

        
    def initialize_data_ingestion(self, ):
        """
        Method Name: initialize_data_ingestion
        Description: Initializes the data ingestion process by downloading JSON files, converting them to Parquet format,
                    and creating the data ingestion artifact.
        Revision:
        Returns:
            DataIngestionArtifact: Data ingestion artifact containing relevant file paths and metadata information.
        On Failure: Raises FinanceException.

        """
        try:
            logger.info("Downloading JSON Files")
            if self.data_ingestion_config.from_date != self.data_ingestion_config.to_date:
                self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                logger.info("Converting and combining downloaded JSON into Parquet file")
                file_path = self.convert_files_to_parquet()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,
                                                self.data_ingestion_config.file_name)
            artifact = DataIngestionArtifact(
                feature_store_file_path=feature_store_file_path,
                download_dir=self.data_ingestion_config.download_dir,
                metadata_file_path=self.data_ingestion_config.metadata_file_path,
            )

            logger.info(f"Data ingestion artifact: {artifact}")
            return artifact
        except Exception as e:
            raise FinanceException(e, sys)

        
if __name__ == '__main__':
    config = FinanceConfig()
    data_ingestion_config = config.get_data_ingestion_config()
    data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config, n_try=5)
    data_ingestion.initialize_data_ingestion()