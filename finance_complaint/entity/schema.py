import os, sys
from typing import List
from pyspark.sql.types import StructType, StructField, StringType,TimestampType

from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger

class FinanceDataSchema:
    def __init__(self):
        try:
            self.col_company = "company"
            self.col_company_public_response = "company_public_response"
            self.col_company_response = "company_response"
            self.col_complaint_id = "complaint_id"
            self.col_complaint_what_happened = "complaint_what_happened"
            self.col_consumer_consent_provided = "consumer_consent_provided"
            self.col_consumer_disputed = "consumer_disputed"
            self.col_date_received = "date_received"
            self.col_date_sent_to_company = "date_sent_to_company"
            self.col_issue = "issue"
            self.col_product = "product"
            self.col_state = "state"
            self.col_sub_issue = "sub_issue"
            self.col_sub_product = "sub_product"
            self.col_submitted_via = "submitted_via"
            self.col_tags = "tags"
            self.col_timely = "timely"
            self.col_zip_code = "zip_code"
        except Exception as e:
            raise FinanceException(e, sys)

    def dataframe_schema(self):
        try:
            schema = StructType([
                StructField(self.col_company, StringType()),
                StructField(self.col_company_public_response, StringType()),
                StructField(self.col_company_response, StringType()),
                StructField(self.col_complaint_id, StringType()),
                StructField(self.col_complaint_what_happened, StringType()),
                StructField(self.col_consumer_consent_provided, StringType()),
                StructField(self.col_consumer_disputed, StringType()),
                StructField(self.col_date_received, TimestampType()),  # Modify the data type to TimestampType()
                StructField(self.col_date_sent_to_company, TimestampType()),  # Modify the data type to TimestampType()
                StructField(self.col_issue, StringType()),
                StructField(self.col_product, StringType()),
                StructField(self.col_state, StringType()),
                StructField(self.col_sub_issue, StringType()),
                StructField(self.col_sub_product, StringType()),
                StructField(self.col_submitted_via, StringType()),
                StructField(self.col_tags, StringType()),
                StructField(self.col_timely, StringType()),
                StructField(self.col_zip_code, StringType())
            ])
            return schema
        except Exception as e:
            raise FinanceException(e, sys)

    
    @property
    def target_column(self):
        column = self.col_consumer_disputed
        return column
    
    @property
    def one_hot_encoding_features(self) -> List[str]:
        features = [
            self.col_company_response,
            self.col_consumer_consent_provided,
            self.col_submitted_via,
        ]
        return features

    @property
    def im_one_hot_encoding_features(self) -> List[str]:
        return [f"im_{col}" for col in self.one_hot_encoding_features]

    @property
    def string_indexer_one_hot_features(self) -> List[str]:
        return [f"si_{col}" for col in self.one_hot_encoding_features]

    @property
    def tf_one_hot_encoding_features(self) -> List[str]:
        return [f"tf_{col}" for col in self.one_hot_encoding_features]
    
    
    @property
    def tfidf_features(self) -> List[str]:
        features = [
            self.col_issue
        ]
        return features

    @property
    def required_columns(self) -> List[str]:
        features = [self.target_column] + self.one_hot_encoding_features + self.tfidf_features + \
                   [self.col_date_sent_to_company, self.col_date_received]
    
        return features


    @property
    def required_columns(self) -> List[str]:
        features = [self.target_column] + self.one_hot_encoding_features + self.tfidf_features + \
                   [self.col_date_sent_to_company, self.col_date_received]
        return features


    @property
    def unwanted_columns(self) -> List[str]:
        features = [
            self.col_complaint_id,
            self.col_sub_product,
            self.col_complaint_what_happened]
        
        return features
    

if __name__ == "__main__":
    f = FinanceDataSchema()
    print(f.required_columns)