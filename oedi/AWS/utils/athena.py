from pyathena.connection import Connection
from pyathena.pandas_cursor import PandasCursor


class OEDIAthena(object):
    """
    OEDI Athena client
    """
    def __init__(self, staging_location=None, region_name=None):
        """Create OEDI Athena class instance"""
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
            self._conn = conn = Connection(
                region_name=self.region_name,
                s3_staging_dir=self.staging_location
            )
        return self._conn

    def __exit__(self):
        self.conn.close()

    def run_query(self, query_string, output_file=None):
        """Run SQL query using pyathena."""
        cursor = self.conn.cursor(PandasCursor)
    
        try:
            result = cursor.execute(query_string).as_pandas()
        except Exception as e:
            print(str(e))
            return
        finally:
            cursor.close()
        
        if output_file:
            result.to_csv(output_file, index=False)
            return True

        return result
