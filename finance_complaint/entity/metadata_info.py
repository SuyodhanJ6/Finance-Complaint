import os
import sys

from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from finance_complaint.utils import *
from finance_complaint.entity.config_entity import DataIngestionMetadataInfo


class DataIngestionMetadata:
    """
    Class Name: DataIngestionMetadata
    Description: Handles metadata operations for data ingestion.
    Author Name: prashantmalge181@gmail.com
    Revision:
    """

    def __init__(self, metadata_file_path: str):
        """
        Method Name: __init__
        Description: Initializes the DataIngestionMetadata instance.
        Args:
            metadata_file_path (str): File path of the metadata file.
        Returns:
            None
        On Failure:
            Raises a FinanceException with detailed error information.
        Revision:
        """
        try:
            self.metadata_file_path = metadata_file_path
        except Exception as e:
            raise FinanceException(e, sys)

    @property
    def is_meta_data_available(self):
        """
        Method Name: is_meta_data_available
        Description: Checks if metadata file is available.
        Returns:
            bool: True if metadata file exists, False otherwise.
        On Failure:
            Raises a FinanceException with detailed error information.
        Revision:
        """
        try:
            return os.path.exists(self.metadata_file_path)
        except Exception as e:
            raise FinanceException(e, sys)

    def write_meta_info(self, from_date: str, to_date: str, data_file_path: str):
        """
        Method Name: write_meta_info
        Description: Write metadata information to the metadata file.
        Args:
            from_date (str): Start date for data ingestion.
            to_date (str): End date for data ingestion.
            data_file_path (str): File path of the data file.
        Returns:
            None
        On Failure:
            Raises a FinanceException with detailed error information.
        Revision:
        """
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date=from_date,
                to_date=to_date,
                data_file_path=data_file_path
            )
            write_yaml_file(self.metadata_file_path, metadata_info)
        except Exception as e:
            raise FinanceException(e, sys)

    def get_metadata_info(self):
        """
        Method Name: get_metadata_info
        Description: Get the metadata information from the metadata file.
        Returns:
            DataIngestionMetadataInfo: Object containing metadata information.
        On Failure:
            Raises a FinanceException with detailed error information.
        Revision:
        """
        try:
            if not self.is_meta_data_available:
                raise Exception("Metadata file not found")

            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadataInfo(**metadata)
            logger.info(f"Metadata information: {metadata_info}")
            return metadata_info
        except Exception as e:
            raise FinanceException(e, sys)
