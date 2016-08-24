import sys

import logging.config
from yapsy.PluginManager import PluginManager
from output_plugin import output_plugin
from datetime import datetime
from pytz import timezone
import time

class wq_prediction_engine(object):
  def __init__(self):
    self.logger = logging.getLogger(__name__)

  def initialize_engine(self, **kwargs):
    raise "Child class must instantiate"

  def build_test_objects(self, **kwargs):
    raise "Child class must instantiate"

  def run_wq_models(self, **kwargs):
    raise "Child class must instantiate"

  def run_output_plugins(self, **kwargs):

    self.logger.info("Begin run_output_plugins")

    simplePluginManager = PluginManager()
    logging.getLogger('yapsy').setLevel(logging.DEBUG)
    simplePluginManager.setCategoriesFilter({
       "OutputResults": output_plugin
       })

    # Tell it the default place(s) where to find plugins
    self.logger.debug("Plugin directories: %s" % (kwargs['output_plugin_directories']))
    simplePluginManager.setPluginPlaces(kwargs['output_plugin_directories'])

    simplePluginManager.collectPlugins()

    plugin_cnt = 0
    plugin_start_time = time.time()
    for plugin in simplePluginManager.getAllPlugins():
      self.logger.info("Starting plugin: %s" % (plugin.name))
      if plugin.plugin_object.initialize_plugin(details=plugin.details):
        plugin.plugin_object.emit(prediction_date=kwargs['prediction_date'].astimezone(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S"),
                                  execution_date=kwargs['prediction_run_date'].strftime("%Y-%m-%d %H:%M:%S"),
                                  ensemble_tests=kwargs['site_model_ensemble'])
        plugin_cnt += 1
      else:
        self.logger.error("Failed to initialize plugin: %s" % (plugin.details))
    self.logger.debug("%d output plugins run in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
    self.logger.info("Finished run_output_plugins")
