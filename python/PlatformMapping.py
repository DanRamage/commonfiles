from xenia_obs_map import *




class PlatformMap(json_obs_map):
    def __init__(self):
        self._platform_handle = None
        self._obs_mapping = None
        return
    def from_json(self, platform_handle, observations, **database_parameters):
        self._platform_handle = platform_handle
        try:
            self._obs_mapping = json_obs_map()
            self._obs_mapping.load_json(observations)
            if database_parameters.get('sqlite_database_file', None):
                    self._obs_mapping.build_db_mappings(sqlite_database_file=database_parameters['sqlite_database_file'],
                                                       platform_handle=platform_handle)
            else:
                self._obs_mapping.build_db_mappings(db_connectionstring=database_parameters['db_type'],
                                                    db_user=database_parameters['db_user'],
                                                    db_password=database_parameters['db_password'],
                                                    db_host=database_parameters['db_host'],
                                                    db_name=database_parameters['db_name'],
                                                    platform_handle=platform_handle)
        except Exception as e:
            raise e
        return

    def get_mapping(self, observation_name, s_order=1):
        return None
    @property
    def platform(self):
        return self._platform_handle
    @property
    def observation_mappings(self):
        return self._obs_mapping

class ModelSite:
    def __init__(self, site_name):
        self._site_name = site_name
        self._platform_mappings = {}
        self._platform_parameters = {}

    def from_json(self, platform_handle, platform_config, **database_parameters ):
        '''
        platform_handle is the xenia platform_handle used in the database.
        platform_config is a json object. An example format:
                "nos.8656483.met": {
                    "previous_hours": 72,
                    "observations": [
                    {
                      "target_obs": "wind_speed",
                      "header_column": "wind",
                      "s_order": 1,
                      "source_uom": "m_s-1",
                      "target_uom": "m_s-1"
                    }]
                }
            The entry has to have the observations entry, other keys/value entries are free form.
        **database_parameters are the needed values to connect to the databse, whether it's a sqlite or postgres.
            For a sqlite file, the key/value pair is: sqlite_database_file=<database file path>
            For postgres: db_connectionstring=
                          db_user=
                          db_password=
                          db_host=
                          db_name=
        '''
        if platform_handle not in self._platform_parameters:
            self._platform_parameters[platform_handle] = {}
        # We could have various settings for each platform, like the number of previous hours to get data.
        # We load those settings up just by looping the keys.
        for param in platform_config:
            if param != 'observations':
                if param not in self._platform_parameters:
                    self._platform_parameters[platform_handle][param] = platform_config[param]
            # For the observations parameter, we build the obs map.
            else:
                platform_map = PlatformMap()
                platform_map.from_json(platform_handle, platform_config[param], **database_parameters)
                self._platform_mappings[platform_handle] = platform_map

        return

    def get_platform_parameter(self, platform_handle, parameter):
        parameter_value = None
        if platform_handle in self._platform_parameters and parameter in self._platform_parameters[platform_handle]:
            parameter_value = self._platform_parameters[platform_handle][parameter]
        return parameter_value

    def platform_observation_mapping(self, platform_handle):
        if platform_handle in self._platform_mappings:
            return self._platform_mappings[platform_handle].observation_mappings.obs
        return None
    @property
    def platforms(self):
        return self._platform_mappings.keys()



class ModelSitesPlatforms:
    def __init__(self):
        #This is a dict of the sites for the model. This will be populated with the platforms it uses.
        self._sites = {}

    def from_json_file(self, json_config_file, **database_parameters):
        try:
            with open(json_config_file, "r") as platform_json_file:
                platforms_config_json = json.load(platform_json_file)
                self.from_json(platforms_config_json, **database_parameters)
        except Exception as e:
            raise e
    def from_json(self, platforms_config_json, **database_parameters):
        for site in platforms_config_json:
            if site not in self._sites:
                self._sites[site] = ModelSite(site)

            current_site = platforms_config_json[site]
            for platform_handle in current_site:
                current_platform = current_site[platform_handle]
                self._sites[site].from_json(platform_handle=platform_handle,
                                            platform_config=current_platform,
                                            **database_parameters)

    def get_site(self, site_name):
        if site_name in self._sites:
            return self._sites[site_name]
        return None

    @property
    def sites(self):
        return(self._sites.keys())
