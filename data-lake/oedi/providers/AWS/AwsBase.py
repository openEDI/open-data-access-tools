class AwsBase(object):

    def __init__(self):
        self.tracking_the_sun_bucket = "s3://oedi-dev-tracking-the-sun/"
        self.tracking_the_sun_table_name = "oedi_tracking_the_sun"

        self.pv_rooftops_bucket = "s3://oedi-dev-pv-rooftop"
        self.pv_rooftops_rasd_table_name = "pv_rooftops_rasd"
        self.pv_rooftops_buildings_table_name = "pv_rooftops_buildings"
        self.pv_rooftops_dev_planes_table_name = "pv_rooftops_developable_planes"
        self.pv_rooftops_aspects_table_name = "pv_rooftops_aspects"
        self.rsf_array_bucket = "s3://oedi-rsf-array/"
        self.rsf_array_table_name = 'nrel_rsf_array'
        self.stf_array_bucket = "s3://oedi-stf-array/"
        self.stf_array_table_name = 'nrel_stf_array'
        self.garage_array_bucket = "s3://oedi-garage-array/"
        self.garage_array_table_name = 'nrel_garage_array/'
        self.windsite_array_bucket = "s3://oedi-windsite-array/"
        self.windsite_array_table_name = 'nrel_windsite_array'

