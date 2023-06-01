from dataclasses import dataclass


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
