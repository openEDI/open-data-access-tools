import os
import pathlib

import yaml

OEDI_ROOT_DIR = pathlib.Path(__file__).parents[0]
OEDI_DEFAULT_DATA_LAKE_NAME = "oedi-data-lake"
OEDI_DEFAULT_DATABASE_NAME = "oedi_database"


class OEDIConfig(object):
    
    def __init__(self, config_file):
        self._config_file = config_file
        self._data = self.load(self._config_file)
    
    @property
    def config_file(self):
        """The source file of data lake configuration"""
        return self._config_file

    @property
    def data(self):
        return self._data

    def load(self, config_file):
        """Load OEDI data lake configuration"""
        with open(config_file, "r") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return data

    def dump(self, data, config_file):
        """Dump OEDI data lake configuration"""
        with open(config_file, "w") as f:
            yaml.dump(data, f)


class DataLakeConfig(OEDIConfig):
    PROVIDER = "PROVIDER-AWS"
    
    def __init__(self, config_file):
        super().__init__(config_file=config_file)
        self._data = self.data[self.PROVIDER]
    
    def to_string(self):
        """Dump OEDI data lake configuration to string"""
        template = ""
        with open(self.config_file, "r") as f:
            for line in f.readlines():
                template += line
        return template

    @property
    def aws_region(self):
        return self.data.get("AWS Region", None)

    @property
    def data_lake_name(self):
        return self.data.get("Data Lake Name", OEDI_DEFAULT_DATA_LAKE_NAME)
    
    @property
    def database_name(self):
        return self.data.get("Database Name", OEDI_DEFAULT_DATABASE_NAME)

    @property
    def dataset_locations(self):
        return self.data.get("Dataset Locations", [])
    
    @property
    def staging_location(self):
        return self.data.get("Staging Location", None)


data_lake_config = DataLakeConfig(
    config_file=os.path.join(OEDI_ROOT_DIR, "config.yaml")
)
