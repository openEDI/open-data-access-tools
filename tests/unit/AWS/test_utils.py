from datetime import datetime

from oedi.AWS import utils


def test_format_datetime():
    dt = datetime(2020, 1, 2, 1, 2, 3)
    formated = utils.format_datetime(dt)
    assert formated == "2020-01-02 01:02:03"


def test_generate_crawler_name():
    s3url = "s3://bucket-name/Folder1/dataset_name/"
    crawler_name = utils.generate_crawler_name(s3url)

    expected = "bucket-name-folder1-dataset-name"
    assert crawler_name == expected


def test_parse_s3url():
    s3url = "s3://bucket-name/Folder1/dataset_name/"
    bucket, path = utils.parse_s3url(s3url)

    expected_bucket = "bucket-name"
    expected_path = "Folder1/dataset_name"
    assert bucket == expected_bucket
    assert path == expected_path


def test_generate_table_prefix():
    s3url = "s3://bucket-name/Folder1/dataset_name/"
    prefix = utils.generate_table_prefix(s3url)

    expected = "bucket_name_folder1_"
    assert prefix == expected
