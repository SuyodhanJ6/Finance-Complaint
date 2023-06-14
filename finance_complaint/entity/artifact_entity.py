from dataclasses import dataclass

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




@dataclass
class DataValidationArtifact:
    """
    Represents the artifact information related to the data validation process.

    Attributes:
        accepted_file_path (str): The file path for the accepted data.
        rejected_dir (str): The directory path for the rejected data.
    """

    accepted_file_path: str
    rejected_dir: str




@dataclass
class DataTransformationArtifact:
    """
    Represents the artifact information related to the data transformation process.
    
    Attributes:
        transformed_train_file_path (str): The file path for the transformed train data.
        transformed_test_file_path (str): The file path for the transformed test data.
        exported_pipelined_file_path (str): The file path for the exported pipelined data.
    """

    transformed_train_file_path: str
    transformed_test_file_path: str
    exported_pipelined_file_path: str
