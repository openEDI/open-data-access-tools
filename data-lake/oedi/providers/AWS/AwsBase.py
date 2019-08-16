class AwsBase(object):

    def __init__(self):
        self.tracking_the_sun_bucket = "s3://oedi-dev-tracking-the-sun/"
        self.tracking_the_sun_table_name = "tracking_the_sun"

        self.lead_bucket = "s3://oedi-dev-lead/"
        self.lead_ami68_state_city_county_table_name = "lead_ami68_state_city_county"
        self.lead_ami68_tract_table_name = "lead_ami68_tract"
        self.lead_fpl15_state_city_county_table_name = "lead_fpl15_state_city_county"
        self.lead_fpl15_tract_table_name = "lead_fpl15_tract"

        self.pv_rooftops_bucket = "s3://oedi-dev-pv-rooftop"
        self.pv_rooftops_rasd_table_name = "pv_rooftops_rasd"
        self.pv_rooftops_buildings_table_name = "pv_rooftops_buildings"
        self.pv_rooftops_dev_planes_table_name = "pv_rooftops_developable_planes"
        self.pv_rooftops_aspects_table_name = "pv_rooftops_aspects"
