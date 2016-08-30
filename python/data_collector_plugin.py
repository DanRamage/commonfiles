import logging.config

from yapsy.IPlugin import IPlugin
from multiprocessing import Process

class data_collector_plugin(IPlugin, Process):
  def __init__(self):
    Process.__init__(self)
    IPlugin.__init__(self)
    self.logger = logging.getLogger(type(self).__name__)

  def initialize_plugin(self, **kwargs):
    raise "Must be implemented by child."

  def run(self):
    raise "Must be implemented by child."


