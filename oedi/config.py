import io
import os
import shutil
import yaml

from oedi.exceptions import ConfigFileNotFound

OEDI_CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".oedi")
OEDI_CONFIG_FILE = os.path.join(OEDI_CONFIG_DIR, "config.yaml")
AWS_DEFAULT_DATALAKE_NAME = "oedi_datalake"
AWS_DEFAULT_DATABASE_NAME = "oedi_database"


def init_config():
    """Initialize OEDI using default config."""
    if os.path.exists(OEDI_CONFIG_FILE):
        return

    oedi_defalt_config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "config.yaml"
    )
    os.makedirs(OEDI_CONFIG_DIR, exist_ok=True)
    shutil.copyfile(oedi_defalt_config_file, OEDI_CONFIG_FILE)


class OEDIConfigBase(object):
    """Config Classs for manipulating OEDI configurations"""
    def __init__(self, config_file=None):
        if not config_file or not os.path.exists(config_file):
            config_file = OEDI_CONFIG_FILE
            if not os.path.exists(config_file):
                raise ConfigFileNotFound("Please run 'oedi config init' first.")
        self._config_file = config_file

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
        data = self.load()
        return data[self.provider]

    def load(self):
        """Load OEDI configuration from file."""
        with open(self.config_file, "r") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        return data

    def dump(self, data):
        """Dump OEDI configuration to file."""
        with open(self.config_file, "w") as f:
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
