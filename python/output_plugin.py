import logging.config

from yapsy.IPlugin import IPlugin
#from multiprocessing import Process, Queue, current_process

class output_plugin(IPlugin):
  def __init__(self):
    #Process.__init__(self)
    IPlugin.__init__(self)
    self.logger = None

  def initialize_plugin(self, **kwargs):
    return
  def emit(self):
    raise NotImplementedError("Function must be implemented by child.")
