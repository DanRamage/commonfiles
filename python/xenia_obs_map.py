import logging
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, platform
from datetime import datetime
import json

class obs_map:
  def __init__(self):
    self.__target_obs = None
    self.__target_uom = None
    self.__source_obs = None
    self.__source_uom = None
    self.__source_index = None
    self.__s_order = 1
    self.__sensor_id = None
    self.__m_type_id = None

  @property
  def target_obs(self):
    return self.__target_obs

  @target_obs.setter
  def target_obs(self, target_obs):
    self.__target_obs = target_obs

  @property
  def target_uom(self):
    return self.__target_uom

  @target_uom.setter
  def target_uom(self, target_uom):
    self.__target_uom = target_uom

  @property
  def source_obs(self):
    return self.__source_obs

  @source_obs.setter
  def source_obs(self, source_obs):
    self.__source_obs = source_obs

  @property
  def source_uom(self):
    return self.__source_uom

  @source_uom.setter
  def source_uom(self, source_uom):
    self.__source_uom = source_uom

  @property
  def s_order(self):
    return self.__s_order

  @s_order.setter
  def s_order(self, s_order):
    self.__s_order = s_order

  @property
  def source_index(self):
    return self.__source_index

  @source_index.setter
  def source_index(self, source_index):
    self.__source_index = source_index

  @property
  def sensor_id(self):
    return self.__sensor_id

  @sensor_id.setter
  def source_index(self, sensor_id):
    self.__sensor_id = sensor_id

  @property
  def m_type_id(self):
    return self.__m_type_id

  @m_type_id.setter
  def m_type_id(self, m_type_id):
    self.__m_type_id = m_type_id

class json_obs_map:
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)
    self.obs = []

  def load_json_mapping(self, file_name):
    try:
      with open(file_name, "r") as obs_json:
        obs_json = json.load(obs_json)
        self.load_json(obs_json)
    except Exception as e:
      self.logger.exception(e)
      raise

  def load_json(self, obs_json):
    for obs in obs_json:
      xenia_obs = obs_map()
      xenia_obs.target_obs = obs['target_obs']
      if obs['target_uom'] is not None:
        xenia_obs.target_uom = obs['target_uom']
      xenia_obs.source_obs = obs['header_column']
      if obs['source_uom'] is not None:
        xenia_obs.source_uom = obs['source_uom']
      if obs['s_order'] is not None:
        xenia_obs.s_order = obs['s_order']
      self.obs.append(xenia_obs)

  def build_db_mappings(self, **kwargs):
    db = xeniaAlchemy()
    if (db.connectDB(kwargs['db_connectionstring'],
                     kwargs['db_user'],
                      kwargs['db_password'],
                      kwargs['db_host'],
                      kwargs['db_name'],
                     False) == True):
      self.logger.info("Succesfully connect to DB: %s at %s" % (kwargs['db_name'], kwargs['db_host']))
    else:
      self.logger.error("Unable to connect to DB: %s at %s. Terminating script." % (kwargs['db_name'], kwargs['db_host']))

    entry_date = datetime.now()
    for obs_rec in self.obs:
      if obs_rec.target_obs != 'm_date':
        self.logger.debug("Platform: %s checking sensor exists %s(%s) s_order: %d" % (kwargs['platform_handle'],
                                                                                      obs_rec.target_obs,
                                                                                      obs_rec.target_uom,
                                                                                      obs_rec.s_order))
        sensor_id = db.sensorExists(obs_rec.target_obs, obs_rec.target_uom, kwargs['platform_handle'], obs_rec.s_order)
        if sensor_id is None:
          self.logger.debug("Sensor does not exist, adding")
          platform_id = db.platformExists(kwargs['platform_handle'])
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
  def get_date_field(self):
    for obs in self.obs:
      if obs.target_obs == 'm_date':
        return obs

  def get_rec_from_source_name(self, name):
    for obs in self.obs:
      if obs.source_obs == name:
        return obs
    return None

  def __iter__(self):
    for obs_rec in self.obs:
      yield obs_rec