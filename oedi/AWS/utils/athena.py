from pyathena.connection import Connection
from pyathena.pandas_cursor import PandasCursor


def run_pyathena(query_string, s3_staging_dir, region_name):
    """Run SQL query through PyAthena."""
    conn = Connection(
        region_name=region_name,
        s3_staging_dir=s3_staging_dir
    )
    cursor = conn.cursor(PandasCursor)
    
    try:
        result = cursor.execute(query_string).as_pandas()
    except Exception as e:
        print(str(e))
    finally:
        cursor.close()
        conn.close()
    
    return result
