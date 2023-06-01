import yaml
import os, sys

from finance_complaint.exception import FinanceException

def write_yaml_file(file_path: str, data: dict):
    """
    Writes data to a YAML file.
    Args:
        file_path (str): The path of the YAML file.
        data (dict): The data to be written.

    Raises:
        FinanceException: If there is an error while writing the YAML file.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as yaml_file:
            if data is not None:
                yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e, sys)

def read_yaml_file(file_path: str) -> dict:
    """
    Reads a YAML file and returns the loaded data as a dictionary.
    Args:
        file_path (str): The path of the YAML file.

    Returns:
        dict: The loaded data as a dictionary.

    Raises:
        FinanceException: If there is an error while reading the YAML file.
    """
    try:
        with open(file_path, 'rb') as yaml_file:
            data = yaml.safe_load(yaml_file)
        return data
    except Exception as e:
        raise FinanceException(e, sys)
