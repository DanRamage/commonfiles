import logging
import json
from xeniaSQLAlchemy import xeniaAlchemy

from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy
from datetime import datetime


class obs_map:
    def __init__(self):
        self.target_obs = None
        self.target_uom = None
        self.source_obs = None
        self.source_uom = None
        self.source_index = None
        self.s_order = 1
        self.sensor_id = None
        self.m_type_id = None


class PlatformMap:
    def __init__(self):
        self._platform_handle = None
        self._obs_mapping = []
        self._logger = logging.getLogger()

    def build_mappings(self, platform_handle, observations,
                       sqlite_db_file="",
                       db_type="",
                       xenia_obs_db_user="", xenia_obs_db_password="",
                       xenia_obs_db_host="",
                       xenia_obs_db_name=""
                       ):
        self._platform_handle = platform_handle
        if self.load_json(observations):
            if self.build_database_mappings(
                    sqlite_db_file,
                    db_type,
                    xenia_obs_db_user, xenia_obs_db_password,
                    xenia_obs_db_host,
                    xenia_obs_db_name

            ):
                return True
        return False
    def load_json(self, observations):
        try:
            for obs in observations:
                xenia_obs = obs_map()
                xenia_obs.target_obs = obs['target_obs']
                if obs['target_uom'] is not None:
                    xenia_obs.target_uom = obs['target_uom']
                xenia_obs.source_obs = obs['header_column']
                if obs['source_uom'] is not None:
                    xenia_obs.source_uom = obs['source_uom']
                if obs['s_order'] is not None:
                    xenia_obs.s_order = obs['s_order']
                self._obs_mapping.append(xenia_obs)

            return True
        except Exception as e:
            self._logger.exception(e)
        return False
    def build_database_mappings(self, sqlite_db_file="",
                           db_type="",
                           xenia_obs_db_user="", xenia_obs_db_password="",
                           xenia_obs_db_host="",
                           xenia_obs_db_name=""):
        try:
            db = self.connect_database(sqlite_db_file,
                                       db_type,
                                       xenia_obs_db_user, xenia_obs_db_password,
                                       xenia_obs_db_host,
                                       xenia_obs_db_name)
            if db is not None:
                entry_date = datetime.now()
                for obs_rec in self._obs_mapping:
                    if obs_rec.target_obs != 'm_date':
                        self._logger.debug(
                            "Platform: %s checking sensor exists %s(%s) s_order: %d" % (self._platform_handle,
                                                                                        obs_rec.target_obs,
                                                                                        obs_rec.target_uom,
                                                                                        obs_rec.s_order))
                        sensor_id = db.sensorExists(obs_rec.target_obs,
                                                    obs_rec.target_uom,
                                                    self._platform_handle,
                                                    obs_rec.s_order)
                        if sensor_id is None:
                            self._logger.debug("Sensor does not exist, adding")
                            platform_id = db.platformExists(self._platform_handle)
                            sensor_id = db.newSensor(entry_date.strftime('%Y-%m-%d %H:%M:%S'),
                                                     obs_rec.target_obs,
                                                     obs_rec.target_uom,
                                                     platform_id,
                                                     1,
                                                     0,
                                                     obs_rec.s_order,
                                                     None,
                                                     False)
                        obs_rec.sensor_id = sensor_id
                        m_type_id = db.mTypeExists(obs_rec.target_obs, obs_rec.target_uom)
                        obs_rec.m_type_id = m_type_id
                db.disconnect()
                return True
            return False
        except Exception as e:
            self._logger.exception(e)
        return False

    def connect_database(self, sqlite_db_file="",
                           db_type="",
                           xenia_obs_db_user="", xenia_obs_db_password="",
                           xenia_obs_db_host="",
                           xenia_obs_db_name=""):
        if len(sqlite_db_file) == 0:
            db = xeniaAlchemy()
            if (db.connectDB(db_type,
                             xenia_obs_db_user,
                             xenia_obs_db_password,
                             xenia_obs_db_host,
                             xenia_obs_db_name,
                             False)):
                self._logger.info(f"Succesfully connect to DB: {xenia_obs_db_name} at {xenia_obs_db_host}.")
                return db
            else:
                self._logger.error(
                    f"Unable to connect to DB: {xenia_obs_db_name} at {xenia_obs_db_host}.")
                return None
        else:
            db = sl_xeniaAlchemy()
            if db.connectDB('sqlite', None, None, sqlite_db_file, None, False):
                self._logger.info(f"Succesfully connect to DB: {sqlite_db_file}")
                return db
            else:
                self._logger.error(f"Unable to connect to DB: {sqlite_db_file}")
                return None

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

    def initialize(self, platform_handle,
                           platform_config,
                           sqlite_db_file="",
                           db_type="",
                           xenia_obs_db_user="", xenia_obs_db_password="",
                           xenia_obs_db_host="",
                           xenia_obs_db_name=""):
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
            # For the observation parameter, we build the obs map.
            else:
                try:
                    platform_map = PlatformMap()
                    platform_map.build_mappings(platform_handle,
                                           platform_config[param],
                                           sqlite_db_file,
                                           db_type,
                                           xenia_obs_db_user, xenia_obs_db_password,
                                           xenia_obs_db_host,
                                           xenia_obs_db_name
                                           )
                    self._platform_mappings[platform_handle] = platform_map
                except Exception as e:
                    raise e

        return

    def get_platform_parameter(self, platform_handle, parameter):
        parameter_value = None
        if platform_handle in self._platform_parameters and parameter in self._platform_parameters[platform_handle]:
            parameter_value = self._platform_parameters[platform_handle][parameter]
        return parameter_value

    def platform_observation_mapping(self, platform_handle):
        if platform_handle in self._platform_mappings:
            return self._platform_mappings[platform_handle].observation_mappings
        return None

    @property
    def platforms(self):
        return self._platform_mappings.keys()


class ModelSitesPlatforms:
    def __init__(self):
        # This is a dict of the sites for the model. This will be populated with the platforms it uses.
        self._sites = {}

    def initialize(self, json, json_config_file="",
                   sqlite_db_file="",
                   db_type="",
                   xenia_obs_db_user="", xenia_obs_db_password="",
                   xenia_obs_db_host="",
                   xenia_obs_db_name=""):
        if len(json_config_file):
            json = self.from_json_file(json_config_file)
        self.build_mappings(json,
                            sqlite_db_file,
                            db_type,
                            xenia_obs_db_user, xenia_obs_db_password,
                            xenia_obs_db_host,
                            xenia_obs_db_name
                            )
    def from_json_file(self, json_config_file):
        try:
            with open(json_config_file, "r") as platform_json_file:
                platforms_config_json = json.load(platform_json_file)
                return platforms_config_json
        except Exception as e:
            raise e
        return None

    def build_mappings(self, platforms_config_json,
                           sqlite_db_file="",
                           db_type="",
                           xenia_obs_db_user="", xenia_obs_db_password="",
                           xenia_obs_db_host="",
                           xenia_obs_db_name=""
                       ):
        for site in platforms_config_json:
            if site not in self._sites:
                self._sites[site] = ModelSite(site)

            current_site = platforms_config_json[site]
            for platform_handle in current_site:
                current_platform = current_site[platform_handle]
                self._sites[site].initialize(platform_handle,
                                            current_platform,
                                            sqlite_db_file,
                                            db_type,
                                            xenia_obs_db_user, xenia_obs_db_password,
                                            xenia_obs_db_host,
                                            xenia_obs_db_name)

    def get_site(self, site_name):
        if site_name in self._sites:
            return self._sites[site_name]
        return None

    @property
    def sites(self):
        return (self._sites.keys())
