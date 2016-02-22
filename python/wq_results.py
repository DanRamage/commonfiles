import logging.config
import ConfigParser
"""
This is based on the Python logging configuration. It's not a 100% duplication yet.
The idea is to use a configuration similar to the logging where various output handlers
can be used.
AN example configuration:

[output]
handlers=results_email,results_email_user,results_json,results_csv

[handler_results_email]
class=florida_wq_output.email_wq_results
#Params
args=(Mail server, From Address, To List, Subject Line, User name, password tuple, report template, report output filename, report file base url, logging flag)

[handler_results_json]
class=florida_wq_output.json_wq_results
#Params
#Output file name for json data, logging flag
args=('Predictions.json', True)

[handler_results_csv]
class=florida_wq_output.csv_wq_results
#Params
#Output file name for json data, logging flag
args=('Predictions.csv', True)

"""

def _resolve(name):
    """Resolve a dotted name to a global object."""
    name = name.split('.')
    used = name.pop(0)
    found = __import__(used)
    for n in name:
        used = used + '.' + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)
    return found


class wq_results(object):
  def __init__(self, use_logging):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

  def emit(self, record):
    raise NotImplementedError("Must be implemented by child class")

  def flush(self):
    raise NotImplementedError("Must be implemented by child class")

  def output(self, record):
    self.handle(record)

  def handle(self, record):
    self.emit(record)


class results_exporter(object):
  def __init__(self, use_logging):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)
    self.handler_list = []

  def load_configuration(self, config_filename):
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(config_filename)
      output_list = config_file.get("output", "handlers")
    except ConfigParser.Error, e:
      raise e
    else:
      for output_type in output_list.split(','):
        try:
          klass = config_file.get('handler_%s' % (output_type), 'class')
          args = config_file.get('handler_%s' % (output_type), 'args')
          klass_obj = _resolve(klass)
          args = eval(args)
          klass_obj = klass_obj(*args)
          self.handler_list.append(klass_obj)
        except (ConfigParser.Error, Exception) as e:
          raise e

  def output(self, record):
    for handler in self.handler_list:
      handler.output(record)