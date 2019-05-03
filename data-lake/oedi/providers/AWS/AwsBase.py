class AwsBase(object):

    def __init__(self):

        self.tracking_the_sun_table_name = "oedi_tracking_the_sun"
        self.pv_rooftops_rasd_table_name = "pv_rooftops_rasd"
        self.pv_rooftops_buildings_table_name = "pv_rooftops_buildings"
        self.pv_rooftops_bucket = 's3://oedi-dev-pv-rooftop'