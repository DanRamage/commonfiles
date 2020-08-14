import logging
import logging.config
from threading import Thread, current_thread
from multiprocessing import Process, Queue, Event, current_process
import sys
if sys.version_info[0] < 3:
    import logging.handlers
    from logutils.queue import QueueListener

if sys.version_info[0] < 3:
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
    def __init__(self, log_filename, logname, level=logging.DEBUG, disable_existing_loggers=False):
        self._log_level = level
        self._log_filename = log_filename
        self._disable_existing_loggers = disable_existing_loggers
        self._log_queue = Queue()
        self._log_stop_event = Event()
        self._log_listener = None
        self._logger_name = logname

    def setup_logging(self):
        mp_handler = "%s_handler" % (self._logger_name)

        logging_config = {
            'version': 1,
            'disable_existing_loggers': self._disable_existing_loggers,
            'formatters': {
                'f': {
                    'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'stream': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'f',
                    'level': self._log_level
                },
                'file_handler': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': self._log_filename,
                    'formatter': 'f',
                    'level': self._log_level
                }
            },
            'root': {
                'handlers': ['file_handler'],
                'level': logging.NOTSET,
                'propagate': False
            }
        }
        '''
        self._log_listener = Thread(target=queue_listener_process,
                               name='listener',
                               args=(self._log_queue, self._log_stop_event, logging_config, self._logger_name))
        '''
        print("mpl 1")

        self._log_listener = Process(target=queue_listener_process,
                               name='listener',
                               args=(self._log_queue, self._log_stop_event, logging_config, self._logger_name))
        print("mpl 2")

        self._log_listener.start()
        print("mpl 3")

        log_config_main = {
            'version': 1,
            'disable_existing_loggers': self._disable_existing_loggers,
            'handlers': {
                mp_handler: {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.QueueHandler',
                    'queue': self._log_queue,
                },
            },
            'loggers': {
                self._logger_name: {
                    'handlers': [mp_handler],
                    'level': 'DEBUG',
                    'propagate': False
                }
            }
        }
        print("mpl 4")
        logging.config.dictConfig(log_config_main)
        logger = logging.getLogger(self._logger_name)
        logger.debug("Opening log file.")

    def getLogger(self):
        logging.config.dictConfig(self.getClientConfigDict())
        logger = logging.getLogger(self._logger_name)
        return logger

    def getClientConfigDict(self):
        mp_handler = "%s_handler" % (self._logger_name)
        log_config_main = {
            'version': 1,
            'disable_existing_loggers': self._disable_existing_loggers,
            'handlers': {
                mp_handler: {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.QueueHandler',
                    'queue': self._log_queue,
                },
            },
            'loggers': {
                self._logger_name: {
                    'handlers': [mp_handler],
                    'level': 'DEBUG',
                    'propagate': False
                }
            }
        }
        return log_config_main

    def shutdown_logging(self):
        logger = logging.getLogger(self._logger_name)
        logger.info("Closing log file.")
        self._log_stop_event.set()
        self._log_listener.join()

'''
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
'''

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
        #record.processName = '%s (for %s)' % (current_thread().name, record.processName)
        logger.handle(record)

def queue_listener_process(log_msg_queue, stop_event, config, logger_name):
    """
    This could be done in the main process, but is just done in a separate
    process for illustrative purposes.

    This initialises logging according to the specified configuration,
    starts the listener and waits for the main process to signal completion
    via the event. The listener is then stopped, and the process exits.
    """
    #logging.config.dictConfig(config)
    try:
        #logging.config.fileConfig(config)
        print("qlp 1")
        logging.config.dictConfig(config)
        print("qlp 2")
        logger = logging.getLogger()
        print("qlp 3")
        listener = logging.handlers.QueueListener(log_msg_queue, MyHandler())
        print("qlp 4")
        #que_handler = logging.handlers.QueueHandler(log_msg_queue)
        #listener = logging.handlers.QueueListener(log_msg_queue, que_handler)
        listener.start()
        print("qlp 5")
        logger.debug("Log listener now running.")
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

'''
def queue_listener_process(**kwargs):    
    try:
        # log_msg_queue, stop_event, config, logger_name
        log_msg_queue = kwargs['log_queue']
        stop_event = kwargs['stop_event']
        logger_name = kwargs.get('logger_name', __name__)
        if 'dict_config' in kwargs:
            logging.config.dictConfig(kwargs['dict_config'])
        else:
            logging.config.fileConfig(kwargs['file_config'])
        # logging.config.fileConfig(config)
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
'''