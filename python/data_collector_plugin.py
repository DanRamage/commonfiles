import logging.config

from yapsy.IPlugin import IPlugin
from multiprocessing import Process, Queue
import sys
if sys.version_info[0] < 3:
  from logutils.queue import QueueListener
else:
  from logging.handlers import QueueListener

class data_collector_plugin(IPlugin, Process):
  def __init__(self):
    Process.__init__(self)
    IPlugin.__init__(self)
    self.logger = None
    self.logger = logging.getLogger(type(self).__name__)
    self.plugin_details = None
    self.logging_client_cfg = None
  def initialize_plugin(self, **kwargs):
    plugin_details = kwargs['details']
    #Each plugin needs to handle all logging internally since we are using
    #multiprocessing. WIthout this the run() function logging may or may not
    #work correctly for certain handlers such as file.
    self.log_queue = Queue()
    self.logger_name = self.__class__.__name__
    logger = logging.getLogger(self.logger_name)
    logger_handlers = logger.handlers
    self.log_listener = QueueListener(self.log_queue, *logger_handlers)
    self.log_listener.start()
    self.logging_client_cfg = {
      'version': 1,
      'disable_existing_loggers': False,
      'handlers': {
        'default': {
          'level': 'DEBUG',
          'class': 'logutils.queue.QueueHandler',
          'queue': self.log_queue,
        },
      },
      'loggers': {
        '': {
          'handlers': ['default'],
          'level': 'DEBUG',
          'propagate': False
        },
        self.logger_name: {
          'handlers': ['default'],
          'level': 'DEBUG',
          'propagate': False
        }
      }
    }
    logger.debug("Plugin logging enabled.")
  def run(self):
    raise Exception("Must be implemented by child.")

  def finalize(self):
    self.logger.info("Closing loggers.")
    #self.log_stop_event.set()
    self.log_listener.stop()


