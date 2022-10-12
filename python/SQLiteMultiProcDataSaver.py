import sys
sys.path.append("../commonfiles/python")
import logging.config

import time
import traceback

from multiprocessing import Process, Queue, current_process, Event
from sqlalchemy import exc
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy



class SQLiteMPDataSaver(Process):
  def __init__(self, db_filename, log_config_file, queue):
    Process.__init__(self)
    self._data_queue = queue
    self._sqlite_file = db_filename
    self._log_config_file = log_config_file
    self._stop_event = Event()

  @property
  def data_queue(self):
    return self._data_queue

  def add_records(self, records):
    for rec in records:
      self._data_queue.put(rec)

  def run(self):
    logger = None
    try:
      logging.config.fileConfig(self._log_config_file)
      logger = logging.getLogger()
      logger.info("SQLiteMPDataSaver Log file opened.")

      process_data = True

      db = sl_xeniaAlchemy()
      if(db.connectDB('sqlite', None, None, self._sqlite_file, None, False) == True):
        logger.info("Succesfully connect to DB: {db_file}".format(db_file=self._sqlite_file))
      else:
        logger.error("Unable to connect to DB: {db_file}. Terminating script.".format(db_file=self._sqlite_file))
        process_data = False

      start_time = time.time()
      rec_count = 0
      while process_data:
        print(rec_count)
        data_rec = self._data_queue.get()
        if data_rec is not None:
          try:
            db.session.add(data_rec)
            db.session.commit()
            if ((rec_count % 10) == 0):
              val = ""
              if data_rec.m_value is not None:
                val = "%f" % (data_rec.m_value)
              logger.info(
                "Committing record Sensor: %d Datetime: %s Value: %s" % (data_rec.sensor_id, data_rec.m_date, val))

              try:
                logger.info("Approximate record count in DB queue: %d" % (self._data_queue.qsize()))
              #We get this exception under OSX.
              except NotImplementedError:
                pass

              rec_count += 1
          # Trying to add record that already exists.
          except exc.IntegrityError as e:
            #logger.error("Duplicate sensor id: %d Datetime: %s" % (data_rec.sensor_id, data_rec.m_date))
            db.session.rollback()
          except Exception as e:
            db.session.rollback()
            logger.exception(e)

        else:
          process_data = False
      db.disconnect()
      logger.info("%s completed in %f seconds." % (current_process().name, time.time()-start_time))
    except Exception as e:
      if logger is not None:
        logger.exception(e)
      else:
        traceback.print_exc(e)