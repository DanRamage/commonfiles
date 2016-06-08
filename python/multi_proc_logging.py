import logging
import logging.config
import logging.handlers
from logutils.queue import QueueListener
from multiprocessing import Process, Queue, Event, current_process
import os

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

def listener_process(q, stop_event, config):
    """
    This could be done in the main process, but is just done in a separate
    process for illustrative purposes.

    This initialises logging according to the specified configuration,
    starts the listener and waits for the main process to signal completion
    via the event. The listener is then stopped, and the process exits.
    """
    #logging.config.dictConfig(config)
    logging.config.fileConfig(config)
    #listener = logging.handlers.QueueListener(q, MyHandler())
    listener = QueueListener(q, MyHandler())
    listener.start()
    if os.name == 'posix':
        # On POSIX, the setup logger will have been configured in the
        # parent process, but should have been disabled following the
        # dictConfig call.
        # On Windows, since fork isn't used, the setup logger won't
        # exist in the child, so it would be created and the message
        # would appear - hence the "if posix" clause.
        logger = logging.getLogger('main_logger')
        logger.critical('Should not appear, because of disabled logger ...')
    stop_event.wait()
    logger.info("Logger listener stop event triggered.")
    listener.stop()

"""
def thread_listener_process(queue, log_conf):
    logging.config.fileConfig(log_conf, disable_existing_loggers=True)

    while True:
        try:
            record = queue.get(True)
            if record is None: # We send this as a sentinel to tell the listener to quit.
                break
            print("Log listener: %s" % (record.name))
            logger = logging.getLogger(record.name)
            logger.handle(record) # No level or filter logic applied - just do it!
        except Exception:
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
"""