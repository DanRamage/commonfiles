import os
from distutils.core import setup
from distutils.command.build_py import build_py
import ConfigParser
import traceback
from collections import OrderedDict

class my_build_py(build_py):
  dir_list = OrderedDict({
    'config': 'config',
    'logconf': 'config/logconf',
    'templates': 'config/templates',
    'model_configs': 'config/model_configs',
    'scripts': 'scripts',
    'input_plugins': 'scripts/data_collector_plugins',
    'output_plugins': 'scripts/output_plugins'
  })
  def create_directories(self):
    print("Checking and creating directories.")
    if not self._dry_run:
      for dir_key in self.dir_list:
        try:
          new_dir = os.path.join('./', self.dir_list[dir_key])
          print("Checking if %s directory: %s exists" % (dir_key, new_dir))
          if os.path.isdir(new_dir) == False:
            print("Creating %s directory: %s" % (dir_key, new_dir))
            os.makedirs(new_dir)
        except Exception as e:
          traceback.print_exc(e)

  def create_ini_skeleton(self):
    try:
      ini_cfg = ConfigParser.ConfigParser()
      #Get the script directory.
      script_directory = os.path.dirname(os.path.realpath(__file__))
      #Now get the name of the directory
      script_path, script_dir_name = os.path.split(script_directory)

      ini_cfg.add_section('processing_settings')
      ini_cfg.set('processing_settings','bbox', "")
      ini_cfg.set('processing_settings','worker_process_count', 4)

      ini_cfg.add_section('entero_limits')
      ini_cfg.set('entero_limits','limit_hi', 0)
      ini_cfg.set('entero_limits','limit_lo', 0)

      ini_cfg.add_section('boundaries_settings')
      config_path = os.path.join(script_directory, self.dir_list['config'])

      file_name = os.path.join(config_path, "%s_boundaries.csv" % (script_dir_name))
      ini_cfg.set('boundaries_settings','boundaries_file', file_name)

      file_name = os.path.join(config_path, "%s_sample_sites.csv" % (script_dir_name))
      ini_cfg.set('boundaries_settings','sample_sites', file_name)

      ini_cfg.add_section('logging')
      config_path = os.path.join(script_directory, self.dir_list['logconf'])

      file_name = os.path.join(config_path, '%s_prediction_engine.conf' % (script_dir_name))
      ini_cfg.set('logging','prediction_engine', file_name)
      #Create the log conf file
      self.create_log_file('%s_prediction_engine.log' % (script_dir_name), file_name)

      file_name = os.path.join(config_path, '%s_xmrg_ingest.conf' % (script_dir_name))
      ini_cfg.set('logging','xmrg_ingest', file_name)
      #Create the log conf file
      self.create_log_file('%s_xmrg_ingest.log' % (script_dir_name), file_name)

      file_name = os.path.join(config_path, '%s_wq_sample_data.conf' % (script_dir_name))
      ini_cfg.set('logging','wq_sample_data_log_file', file_name)
      #Create the log conf file
      self.create_log_file('%s_wq_sample_data.log' % (script_dir_name), file_name)

      ini_cfg.add_section('units_conversion')
      ini_cfg.set('units_conversion','config_file', "")

      ini_cfg.add_section('password_protected_configs')
      ini_cfg.set('password_protected_configs','settings_ini', "")

      ini_cfg.add_section('data_collector_plugins')
      config_path = os.path.join(script_directory, self.dir_list['input_plugins'])
      ini_cfg.set('data_collector_plugins','plugin_directories', config_path)

      ini_cfg.add_section('output_plugins')
      config_path = os.path.join(script_directory, self.dir_list['output_plugins'])
      ini_cfg.set('output_plugins','plugin_directories', config_path)
    except Exception as e:
      traceback.print_exc(e)
    else:
      try:
        with open("./config/prediction_config.ini", 'w') as file_obj:
          ini_cfg.write(file_obj)
      except Exception as e:
        traceback.print_exc(e)
    return

  def create_log_file(self, log_file_name, conf_file_name):
    try:
      print("Creating log conf file: %s" % (conf_file_name))
      ini_cfg = ConfigParser.ConfigParser()
      #Get the script directory.
      script_directory = os.path.dirname(os.path.realpath(__file__))

      ini_cfg.add_section('loggers')
      ini_cfg.set('loggers','root', "root")

      ini_cfg.add_section('logger_root')
      ini_cfg.set('logger_root', 'handlers', "file,bufferingsmtp")
      ini_cfg.set('logger_root', 'level', "NOTSET")

      ini_cfg.add_section('formatters')
      ini_cfg.set('formatters', 'keys', "simple, complex")

      ini_cfg.add_section('formatter_simple')
      ini_cfg.set('formatter_simple', 'format', "%(asctime)s,%(levelname)s,%(name)s,%(funcName)s,%(lineno)d,%(message)s")

      ini_cfg.add_section('formatter_complex')
      ini_cfg.set('formatter_complex', 'format', "%(asctime)s,%(levelname)s,%(name)s,%(funcName)s,%(lineno)d,%(message)s")

      ini_cfg.add_section('handlers')
      ini_cfg.set('handlers', 'keys', "file, bufferingsmtp")

      ini_cfg.add_section('handler_file')
      ini_cfg.set('handler_file', 'class', "handlers.RotatingFileHandler")
      ini_cfg.set('handler_file', 'formatter', "complex")
      ini_cfg.set('handler_file', 'level', "DEBUG")
      args = '("%s","a",10000000,5)' % (log_file_name)
      ini_cfg.set('handler_file', 'args', args)

      ini_cfg.add_section('handler_screen')
      ini_cfg.set('handler_screen', 'class', "StreamHandler")
      ini_cfg.set('handler_screen', 'formatter', "complex")
      ini_cfg.set('handler_screen', 'level', "DEBUG")
      ini_cfg.set('handler_screen', 'args', '(sys.stdout,)')

      ini_cfg.add_section('handler_bufferingsmtp')
      ini_cfg.set('handler_bufferingsmtp', 'class', "StreamHandler")
      ini_cfg.set('handler_bufferingsmtp', 'formatter', "complex")
      ini_cfg.set('handler_bufferingsmtp', 'level', "DEBUG")
      ini_cfg.set('handler_bufferingsmtp', 'args', '')

    except Exception as e:
      traceback.print_exc(e)
    else:
      try:
        with open(conf_file_name, 'w') as file_obj:
          ini_cfg.write(file_obj)
      except Exception as e:
        traceback.print_exc(e)

  def run(self):
    self.create_directories()
    self.create_ini_skeleton()
    return


setup(
  cmdclass={'create_basic': my_build_py}

)

