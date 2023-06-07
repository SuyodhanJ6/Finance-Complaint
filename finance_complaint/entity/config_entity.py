from dataclasses import dataclass

############### THIS ALL ABOUT DATA INGESTION ###############

@dataclass
class TrainingPipelineConfig:
    """
    Represents configuration for a training pipeline.

    Attributes:
        pipeline_name (str): Name of the pipeline.
        artifact_dir (str): Directory path for storing pipeline artifacts.
    """
    pipeline_name: str
    artifact_dir: str

@dataclass
class DataIngestionConfig:
    """
    Represents configuration for data ingestion.

    Attributes:
        from_date (str): Start date for data ingestion.
        to_date (str): End date for data ingestion.
        data_ingestion_dir (str): Directory path for storing data ingestion files.
        download_dir (str): Directory path for downloaded data.
        file_name (str): Name of the metadata file.
        feature_store_dir (str): Directory path for the feature store.
        failed_dir (str): Directory path for failed data ingestion.
        metadata_file_path (str): File path for the metadata file.
        datasource_url (str): URL of the data source.
    """
    from_date: str
    to_date: str
    data_ingestion_dir: str
    download_dir: str
    file_name: str
    feature_store_dir: str
    failed_dir: str
    metadata_file_path: str
    datasource_url: str


@dataclass
class DataIngestionMetadataInfo:
    """
    Represents metadata information for data ingestion.

    Attributes:
        from_date (str): Start date of the data ingestion.
        to_date (str): End date of the data ingestion.
        data_file_path (str): File path of the data file.
    """
    from_date: str
    to_date: str
    data_file_path: str

@dataclass
class DownloadUrl:
    """
    Represents a download URL along with the corresponding file path and retry information.
    
    Attributes:
        url (str): The download URL.
        file_path (str): The file path to save the downloaded file.
        n_retry (int): The number of retry attempts for downloading the file.
    """
    url: str
    file_path: str
    n_retry: int



@dataclass
class DataIngestionArtifact:
    """
    Represents the artifact information related to data ingestion process.
    
    Attributes:
        DataValidationArtifact (str): The data validation artifact.
        accepted_file_path (str): The file path for accepted data.
        rejected_dir (str): The directory path for rejected data.
    """
    feature_store_file_path: str
    metadata_file_path: str
    download_dir: str