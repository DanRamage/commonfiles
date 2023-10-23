import os
import sys
sys.path.append("../commonfiles/python")

from multiprocessing import Process, Queue, current_process

import time
import logging.config
from sqlalchemy import exc
from xeniaSQLAlchemy import xeniaAlchemy


class MPDataSaverV2(Process):
    def __init__(self):
        Process.__init__(self)
        self._log_config = None
        self._logger_name = ''
        self._data_queue = None
        self._db_user = None
        self._db_pwd = None
        self._db_host = None
        self._db_name = None
        self._db_connection_type = None

    def initialize(self, **kwargs):
        self._logger_name = kwargs.get('logger_name', 'data_saver_logger')
        self._log_config = kwargs['log_config']
        self._data_queue = kwargs['data_queue']
        self._db_user = kwargs.get('db_user', None)
        self._db_pwd = kwargs.get('db_pwd', None)
        self._db_host = kwargs.get('db_host', None)
        self._db_name = kwargs.get('db_name', None)
        self._db_connection_type = kwargs.get('db_connection_type', None)
        self._records_before_commit = kwargs.get('records_before_commit', 1)

    def add_records(self, records):
        for rec in records:
            self._data_queue.put(rec)

    def run(self):
        logger = None
        try:
            logger_name = self._logger_name
            logger_config = self._log_config
            # Each worker will set its own filename for the filehandler
            base_filename = logger_config['handlers']['file_handler']['filename']
            filename_parts = os.path.split(base_filename)
            filename, ext = os.path.splitext(filename_parts[1])
            worker_filename = os.path.join(filename_parts[0], f"{filename}_{current_process().name.replace(':', '_')}{ext}")
            logger_config['handlers']['file_handler']['filename'] = worker_filename
            logging.config.dictConfig(logger_config)
            logger = logging.getLogger(logger_name)
            logger.debug(f"{current_process().name} starting data saver worker.")

            logger = logging.getLogger()
            logger.debug(f"{current_process().name} starting run.")

            process_data = True

            db = xeniaAlchemy()
            if (db.connectDB(self._db_connection_type, self._db_user, self._db_pwd, self._db_host, self._db_name,
                             False)):
                logger.info(f"Successfully connect to DB: {self._db_name} at {self._db_host}")
            else:
                logger.error(f"Unable to connect to DB: {self._db_name} at {self._db_host}. Terminating process.")
                process_data = False

            start_time = time.time()
            rec_count = 0
            while process_data:
                data_rec = self._data_queue.get()
                if data_rec is not None:
                    try:
                        db.session.add(data_rec)
                        if (rec_count % self._records_before_commit) == 0:
                            db.session.commit()

                        val = ""
                        if data_rec.m_value is not None:
                             val = "%f" % (data_rec.m_value)
                        logger.debug(
                            f"Adding record Sensor: {data_rec.sensor_id} Datetime: {data_rec.m_date} Value: {val}")

                        if ((rec_count % 10) == 0):
                            try:
                                logger.debug(f"Approximate record count in DB queue: {self._data_queue.qsize()}")
                            # We get this exception under OSX.
                            except NotImplementedError:
                                pass

                            rec_count += 1
                    # Trying to add record that already exists.
                    except exc.IntegrityError as e:
                        #logger.error(f"Duplicate sensor id: {data_rec.sensor_id} Datetime: {data_rec.m_date}")
                        db.session.rollback()
                    except Exception as e:
                        db.session.rollback()
                        logger.exception(e)

                else:
                    process_data = False
                    db.session.commit()

                db.disconnect()
            logger.debug(f"{current_process().name} completed in {time.time() - start_time} seconds.")
        except Exception as e:
            logger.exception(e)
