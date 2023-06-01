from finance_complaint.config.pipeline.training import FinanceConfig
from datetime import datetime
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.constant import *
from finance_complaint.component.training.data_ingestion import DataIngestion
import os, sys

def run_data_ingestion():
    try:
        finance = FinanceConfig()
        data_ingestion_config = finance.get_data_ingestion_config()
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        data_ingestion.get_required_intervals()
        print(run_data_ingestion())

    except Exception as e:
        raise FinanceException(e, sys)