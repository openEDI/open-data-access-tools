import io
import os
import yaml

DEFAULTT_OEDI_CONFIG_FILLE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config.yaml"
)
AWS_DEFAULT_DATALAKE_NAME = "oedi_datalake"
AWS_DEFAULT_DATABASE_NAME = "oedi_database"


class OEDIConfigBase(object):
    """Config Classs for manipulating OEDI configurations"""
    
    def __init__(self, config_file=None):
        self._config_file = config_file or DEFAULTT_OEDI_CONFIG_FILLE
    
    @property
    def provider(self):
        """The provider of OEDI data lake."""
        raise NotImplementedError
    
    @property
    def config_file(self):
        """The source file of OEDI configuration"""
        return self._config_file

    @property
    def data(self):
        data = self.load(self._config_file)
        return data[self.provider]
    
    def load(self, config_file):
        """Load OEDI configuration from file."""
        with open(config_file, "r") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return data
    
    def dump(self, data, config_file):
        """Dump OEDI configuration to file."""
        with open(config_file, "w") as f:
            yaml.dump(data, f)
    
    def to_string(self):
        """Dump OEDI data lake configuration to string"""
        buffer = io.StringIO()
        yaml.dump(self.data, buffer)
        buffer.seek(0)
        
        template = ""
        for line in buffer.readlines():
            template += line
        return template


class AWSDataLakeConfig(OEDIConfigBase):
    """AWS data lake configuration class"""
    
    @property
    def provider(self):
        return "AWS"
    
    @property
    def region_name(self):
        return self.data.get("Region Name", None)

    @property
    def datalake_name(self):
        return self.data.get("Datalake Name", AWS_DEFAULT_DATALAKE_NAME)
    
    @property
    def database_name(self):
        return self.data.get("Database Name", AWS_DEFAULT_DATABASE_NAME)

    @property
    def dataset_locations(self):
        return self.data.get("Dataset Locations", [])
    
    @property
    def staging_location(self):
        return self.data.get("Staging Location", None)
