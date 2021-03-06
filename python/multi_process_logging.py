import logging
import logging.config
import logging.handlers
from multiprocessing import Process, Queue, Event, current_process
from logutils.queue import QueueListener


class QueueHandler(logging.Handler):
  """
  This is a logging handler which sends events to a multiprocessing queue.

  The plan is to add it to Python 3.2, but this can be copy pasted into
  user code for use with earlier Python versions.
  """

  def __init__(self, queue):
    """
    Initialise an instance, using the passed queue.
    """
    logging.Handler.__init__(self)
    self.queue = queue

  def emit(self, record):
    """
    Emit a record.

    Writes the LogRecord to the queue.
    """
    try:
      ei = record.exc_info
      if ei:
        dummy = self.format(record)  # just to get traceback text into record.exc_text
        record.exc_info = None  # not needed any more
      self.queue.put_nowait(record)
    except (KeyboardInterrupt, SystemExit):
      raise
    except:
      self.handleError(record)

class MainLogConfig:
  def __init__(self, logging_queue, log_filename, level=logging.DEBUG, disable_existing_loggers=False):
    self._queue = logging_queue
    self._log_level = level
    self._log_filename = log_filename
    self._disable_existing_loggers = disable_existing_loggers

  def config_dict(self):
    config = dict(
      version=1,
      disable_existing_loggers=self._disable_existing_loggers,
      formatters={
        'f': {
          'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
          'datefmt': '%Y-%m-%d %H:%M:%S'
        }
      },
      handlers={
        'stream': {
          'class': 'logging.StreamHandler',
          'formatter': 'f',
          'level': self._log_level
        },
        'file_handler': {
          'class': 'logging.handlers.RotatingFileHandler',
          'filename': self._log_filename,
          'formatter': 'f',
          'level': self._log_level,
          'maxBytes': 5000000,
          'backupCount': 3
        }
      },
      root={
        'handlers': ['stream', 'file_handler'],
        'level': logging.NOTSET,
      },
    )
    return config


class ClientLogConfig:
  def __init__(self, logging_queue, logger_name='', level=logging.DEBUG, disable_existing_loggers=False):
    self._queue = logging_queue
    self._log_level = level
    self._logger_name = logger_name
    self._disable_existing_loggers = disable_existing_loggers

  def config_dict(self):
    logging_config = {
        'version': 1,
        'disable_existing_loggers': self._disable_existing_loggers,
        'handlers': {
          'default': {
            'class': 'multi_process_logging.QueueHandler',
            'queue': self._queue,
          },
        },
        'loggers': {
          self._logger_name: {
            'handlers': ['default'],
            'level': self._log_level
          }
        }
      }
    return logging_config

class MyHandler(object):
    """
    A simple handler for logging events. It runs in the listener process and
    dispatches events to loggers based on the name in the received record,
    which then get dispatched, by the logging system, to the handlers
    configured for those loggers.
    """
    def handle(self, record):
        logger = logging.getLogger(record.name)
        # The process name is transformed just to show that it's the listener
        # doing the logging to files and console
        record.processName = '%s (for %s)' % (current_process().name, record.processName)
        logger.handle(record)

def queue_listener_process(**kwargs):
    """
    This could be done in the main process, but is just done in a separate
    process for illustrative purposes.

    This initialises logging according to the specified configuration,
    starts the listener and waits for the main process to signal completion
    via the event. The listener is then stopped, and the process exits.
    """
    #logging.config.dictConfig(config)
    try:
        #log_msg_queue, stop_event, config, logger_name
        log_msg_queue = kwargs['log_queue']
        stop_event = kwargs['stop_event']
        logger_name = kwargs.get('logger_name', __name__)
        if 'dict_config' in kwargs:
            logging.config.dictConfig(kwargs['dict_config'])
        else:
            logging.config.fileConfig(kwargs['file_config'])
        #logging.config.fileConfig(config)
        logger = logging.getLogger(logger_name)
        listener = QueueListener(log_msg_queue, MyHandler())
        listener.start()
        logger.info("Log listener now running.")
        """
        if os.name == 'posix':
            # On POSIX, the setup logger will have been configured in the
            # parent process, but should have been disabled following the
            # dictConfig call.
            # On Windows, since fork isn't used, the setup logger won't
            # exist in the child, so it would be created and the message
            # would appear - hence the "if posix" clause.
            logger.critical('Should not appear, because of disabled logger ...')
        """
        stop_event.wait()
        logger.debug('Logger listener shutting down.')
        listener.stop()
    except Exception as e:
        print('Failed to Create Logging Listener Process')
        raise
        

