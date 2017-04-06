from sqlalchemy import exc
from multiprocessing import Process, Queue, current_process, Event
import logging.config
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, platform

class mp_data_save_worker(Process):
  def __init__(self, **kwargs):
    Process.__init__(self)

    self.logger = logging.getLogger(type(self).__name__)
    self.input_queue = kwargs['input_queue']
    self.db_user = kwargs.get('db_user')
    self.db_pwd = kwargs.get('db_password')
    self.db_host = kwargs.get('db_host')
    self.db_name = kwargs.get('db_name')
    self.db_conn_type = kwargs.get('db_connectionstring')

  def run(self):
    try:
      logger = logging.getLogger(type(self).__name__)
      logger.debug("%s starting process" % (current_process().name))
      process_data = True

      db = xeniaAlchemy()
      if (db.connectDB(self.db_conn_type, self.db_user, self.db_pwd, self.db_host, self.db_name, False) == True):
        if (logger):
          logger.info("Succesfully connect to DB: %s at %s" % (self.db_name, self.db_host))
      else:
        logger.error("Unable to connect to DB: %s at %s. Terminating script." % (self.db_name, self.db_host))
        process_data = False
      if process_data:
        rec_count = 0
        for db_rec in iter(self.input_queue.get, 'STOP'):
          try:
            db.addRec(db_rec, True)
            val = ""
            if (db_rec.m_value != None):
              val = "%f" % (db_rec.m_value)
            logger.debug(
              "Committing record Sensor: %d Datetime: %s Value: %s" % (db_rec.sensor_id, db_rec.m_date, val))
            # Trying to add record that already exists.
          except exc.IntegrityError, e:
            db.session.rollback()
          except Exception, e:
            db.session.rollback()
            logger.exception(e)
        rec_count += 1

      logger.info("%s thread exiting." % (current_process().name))

      db.disconnect()

    except Exception as e:
      logger.exception(e)
    return