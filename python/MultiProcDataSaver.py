import os
import sys
sys.path.append("../commonfiles/python")

from multi_process_logging import ClientLogConfig
from multiprocessing import Process, Queue, current_process

import time
import logging.config
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
import traceback

#import shapely
#from shapely.geometry import Polygon, Point

from sqlalchemy import exc
from sqlalchemy.orm.exc import *
from sqlalchemy import or_
from sqlalchemy.sql import column
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, organization, platform, uom_type, obs_type, m_scalar_type, m_type, sensor


class MPDataSaver(Process):
  def __init__(self, data_queue, logging_queue, db_settings_ini):
    Process.__init__(self)
    self._data_queue = data_queue
    self._logging_queue = logging_queue

    config_file = ConfigParser.RawConfigParser()
    config_file.read(db_settings_ini)

    self._db_user = config_file.get('Database', 'user')
    self._db_pwd = config_file.get('Database', 'password')
    self._db_host = config_file.get('Database', 'host')
    self._db_name = config_file.get('Database', 'name')
    self._db_connection_type = config_file.get('Database', 'connectionstring')

  def add_records(self, records):
    for rec in records:
      self._data_queue.put(rec)

  def run(self):
    logger = None
    try:
      log_config = ClientLogConfig(self._logging_queue)
      logging.config.dictConfig(log_config.config_dict())

      logger = logging.getLogger()
      logger.debug("%s starting run." % (current_process().name))

      process_data = True

      db = xeniaAlchemy()
      if(db.connectDB(self._db_connection_type, self._db_user, self._db_pwd, self._db_host, self._db_name, False) == True):
        logger.info("Succesfully connect to DB: %s at %s" %(self._db_name,self._db_host))
      else:
        logger.error("Unable to connect to DB: %s at %s. Terminating script." %(self._db_name,self._db_host))
        process_data = False

      start_time = time.time()
      rec_count = 0
      while process_data:
        data_rec = self._data_queue.get()
        if data_rec is not None:
          try:
            db.session.add(data_rec)
            db.session.commit()
            val = ""
            if data_rec.m_value is not None:
              val = "%f" % (data_rec.m_value)
            logger.debug(
              "Committing record Sensor: %d Datetime: %s Value: %s" % (data_rec.sensor_id, data_rec.m_date, val))

            if ((rec_count % 10) == 0):
              try:
                logger.debug("Approximate record count in DB queue: %d" % (self._data_queue.qsize()))
              #We get this exception under OSX.
              except NotImplementedError:
                pass

              rec_count += 1
          # Trying to add record that already exists.
          except exc.IntegrityError as e:
            logger.error("Duplicate sensor id: %d Datetime: %s" % (data_rec.sensor_id, data_rec.m_date))
            db.session.rollback()
          except Exception as e:
            db.session.rollback()
            logger.exception(e)

        else:
          process_data = False
      db.disconnect()
      logger.debug("%s completed in %f seconds." % (current_process().name, time.time()-start_time))
    except Exception as e:
      if logger is not None:
        logger.exception(e)
      else:
        traceback.print_exc(e)
