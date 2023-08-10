"""
CDK could not update the tags on crawler after data lake deployment, here we use script to update the tags
"""

import boto3

from oedi import __version__
from oedi.AWS.base import AWSClientBase
from oedi.config import AWSDataLakeConfig


class AWSGlueCrawlerTagManager(AWSClientBase):

    def __init__(self):
        config = AWSDataLakeConfig()
        super().__init__("glue", config.region_name)
    
    def get_current_account_id(self):
        sts = boto3.client("sts")
        return sts.get_caller_identity()["Account"]

    def list_crawler_arns(self):
        account_id = self.get_current_account_id()
        response1 = self.client.list_crawlers(MaxResults=1000, Tags={"Project": "OEDI"})
        crawler_arns = []
        for crawler_name in response1["CrawlerNames"]:
            arn = f"arn:aws:glue:{self.region_name}:{account_id}:crawler/{crawler_name}"
            crawler_arns.append(arn)
        return crawler_arns
    
    def update_crawler_tags(self, crawler_arns):
        tags = {"Release": __version__}
        for crawler_arn in crawler_arns:
            self.client.tag_resource(ResourceArn=crawler_arn, TagsToAdd=tags)

    

if __name__ == "__main__":
    manager = AWSGlueCrawlerTagManager()
    crawler_arns = manager.list_crawler_arns()
    manager.update_crawler_tags(crawler_arns)
    print("Crawler tags update done!")
