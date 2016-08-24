import logging.config

from yapsy.IPlugin import IPlugin
from multiprocessing import Process, Queue, current_process

class wq_prediction_engine_plugin(IPlugin, Process):
  def __init__(self):
    Process.__init__(self)
    IPlugin.__init__(self)
    self.logger = None
    #self.logger = logging.getLogger(type(self).__name__)

  def initialize_plugin(self, **kwargs):
    raise NotImplementedError("Function must be implemented by child.")

  def run_wq_models(self, **kwargs):
    raise NotImplementedError("Function must be implemented by child.")

  def run(self):
    return