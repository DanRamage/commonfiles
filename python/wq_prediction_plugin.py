import logging.config

from yapsy.IPlugin import IPlugin
from multiprocessing import Process
from datetime import datetime
from pytz import timezone
import ConfigParser

class wq_prediction_engine_plugin(IPlugin, Process):
  def __init__(self):
    Process.__init__(self)
    IPlugin.__init__(self)
    self.logger = logging.getLogger(type(self).__name__)

  def initialize_plugin(self, **kwargs):
    try:
      self.logger.debug("inititalize_plugin Started")
      self.config_file = kwargs['ini']
      self.process_dates = kwargs.get('process_date', None)
      self.name = kwargs['name']
      self.logger.debug("inititalize_plugin Finished")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    config_file = ConfigParser.RawConfigParser()
    config_file.read(self.config_file)
    logConfFile = config_file.get('logging', 'prediction_engine')

    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger(self.name)
    logger.debug("run_wq_models Started. Process: %s" % (self.name))
    dates_to_process = []
    if self.process_dates is not None:
      #Can be multiple dates, so let's split on ','
      collection_date_list = self.process_dates.split(',')
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      try:
        for collection_date in collection_date_list:
          est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
          #Convert to UTC
          begin_date = est.astimezone(timezone('UTC'))
          dates_to_process.append(begin_date)
      except Exception,e:
        self.logger.exception(e)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      est = datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))
      dates_to_process.append(begin_date)

    try:
      self.do_processing(processing_dates=dates_to_process)
    except Exception as e:
      logger.exception(e)

    logger.debug("run_wq_models Finished")
    #Force the logging handlers to close. For whatever reason, most likely
    #multiprocessing, they can remain open and not send out remaining messages.
    [hand.close() for hand in logger.root.handlers]
    return

  def do_processing(self, **kwargs):
    raise "Must be implemented by child."

