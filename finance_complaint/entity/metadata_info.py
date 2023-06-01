import os
import sys

from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
from finance_complaint.utils import *
from finance_complaint.entity.config_entity import DataIngestionMetadataInfo
class DataIngestionMetadata:
    def __init__(self, metadata_file_path: str):
        try:
            self.metadata_file_path = metadata_file_path
        except Exception as e:
            raise FinanceException(e, sys)

    @property
    def is_meta_data_available(self):
        try:
            return os.path.exists(self.metadata_file_path)
        except Exception as e:
            raise FinanceException(e, sys)
        

    def write_meta_info(self, from_date: str, to_date: str, data_file_path: str):
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date =  from_date,
                to_date = to_date,
                data_file_path = data_file_path
            )
            write_yaml_file(self.metadata_file_path, metadata_info)
        except Exception as e:
            raise FinanceException(e, sys)

    def get_metadata_info(self):
        try:
            if not self.is_meta_data_available:
                raise Exception("Metadata file not found")

            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadata(**metadata)
            logger.info(f"Metadata information : {metadata_info}")
            return metadata_info
        except Exception as e:
            raise FinanceException(e, sys)
