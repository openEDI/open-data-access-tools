import geopandas

from pyathena.connection import Connection
from pyathena.pandas.cursor import PandasCursor
from shapely import wkt

from oedi.AWS.base import AWSClientBase


class OEDIAthena(AWSClientBase):
    """
    OEDI Athena client
    """
    def __init__(self, staging_location=None, region_name=None, **kwargs):
        """Create OEDI Athena class instance"""
        super().__init__("athena", **kwargs)
        self._staging_location = staging_location
        self._region_name = region_name
        self._conn = None

    @property
    def staging_location(self):
        return self._staging_location

    @property
    def region_name(self):
        return self._region_name

    @property
    def conn(self):
        if not self._conn:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        return Connection(
            region_name=self.region_name,
            s3_staging_dir=self.staging_location
        )

    def __exit__(self):
        if self._conn:
            self._conn.close()

    def __del__(self):
        if self._conn:
            self._conn.close()

    def run_query(self, query_string, geometry=None, pandas_cursor=True):
        """Run SQL query using pyathena."""
        if pandas_cursor:
            result = self._pandas_cursor_execute(query_string)
            if geometry:
                result = self._load_wkt(df=result, geometry=geometry)
        else:
            result = self._cursor_execute(query_string)

        return result

    def _cursor_execute(self, query_string):
        cursor = self.conn.cursor()
        try:
            result = cursor.execute(query_string)
            return result.fetchall()
        finally:
            cursor.close()

    def _pandas_cursor_execute(self, query_string):
        cursor = self.conn.cursor(PandasCursor)
        try:
            result = cursor.execute(query_string)
            return result.as_pandas()
        finally:
            cursor.close()

    def _load_wkt(self, df, geometry):
        """Convert pandas dataframe with WKT column to geopandas geodataframe"""
        df[geometry] = df[geometry].apply(wkt.loads)
        gdf = geopandas.GeoDataFrame(df, geometry=geometry)
        return gdf
