import os
import sys
import logging.config
from datetime import datetime,timedelta
from pytz import timezone
from wqHistoricalData import tide_data_file
from NOAATideData import noaaTideDataExt
import time
from multiprocessing import Process, Queue, current_process, Event
from multi_proc_logging import listener_process



def get_tide_data(**kwargs):
  try:
    processing_start_time = time.time()

    logging.config.dictConfig(kwargs['worker_log_config'])

    logger = logging.getLogger()
    logger.info("%s starting get_tide_data." % (current_process().name))

    inputQueue = kwargs['input_queue']
    resultsQueue = kwargs['results_queue']
    tide_station = kwargs['tide_station']
    write_tide_data = kwargs.get('debug_data', False)

    tide = noaaTideDataExt(use_raw=True, logger=logger)
    rec_cnt = 0
    for date_rec in iter(inputQueue.get, 'STOP'):

      wq_utc_date = date_rec.astimezone(timezone('UTC'))
      tide_start_time = (wq_utc_date - timedelta(hours=24))
      tide_end_time = wq_utc_date

      """
      date_key = date_data_utc.strftime('%Y-%m-%dT%H:%M:%S')
      if len(initial_tide_data):
        if date_key in initial_tide_data:
          if logger:
            logger.debug("Station: %s date: %s in history file, not retrieving." % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))
          tide_csv_file.write("%s,%s,%f,%f,%f\n"\
                               % (tide_station,
                                  date_data_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                                  initial_tide_data[date_key]['range'],
                                  initial_tide_data[date_key]['hi'],
                                  initial_tide_data[date_key]['lo']))

          tide_start_time = None
      else:
        if logger:
          logger.debug("Station: %s date: %s not in history file, retrieving." % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))
      """
      logger.debug("%s Start retrieving tide data for station: %s date: %s-%s" % (current_process().name, tide_station, tide_start_time, tide_end_time))
      if tide_start_time is not None:
        successful = False
        for x in range(0, 5):
          if logger:
            logger.debug("%s Attempt: %d retrieving tide data for station." % (current_process().name, x+1))
          pda_tide_data = tide.calcTideRangePeakDetect(beginDate = tide_start_time,
                             endDate = tide_end_time,
                             station=tide_station,
                             datum='MLLW',
                             units='feet',
                             timezone='GMT',
                             smoothData=False,
                             write_tide_data=write_tide_data)

          '''
          tide_stage = tide.get_tide_stage(begin_date = tide_start_time,
                             end_date = tide_end_time,
                             station=tide_station,
                             datum='MLLW',
                             units='feet',
                             time_zone='GMT',
                             write_tide_data=True)
          '''

          if pda_tide_data is not None:
            pda_tide_data['date'] = wq_utc_date
            resultsQueue.put(pda_tide_data)
            successful = True
            break
        if not successful:
          if logger:
            logger.error("Unable to retrieve data for: %s" % (wq_utc_date))
            #logger.error("Unable to retrieve data for: %s, putting back on queue" % (wq_utc_date))
            #inputQueue.put(wq_utc_date)

        rec_cnt += 1
    logger.info("%s finished get_tide_data. Processed: %d dates in %f seconds" % (current_process().name, rec_cnt, time.time()-processing_start_time))

  except Exception as e:
    if logger:
      logger.exception(e)
  return

def create_tide_data_file_mp(tide_station,
                             date_list,
                             output_file,
                             worker_process_count,
                             logfile,
                             write_debug_file):
  logger = logging.getLogger(__name__)

  try:
    #Open the file and read any tide entries that exist. We'll then just try and fill in the missing
    #ones.
    #initial_tide_data = tide_data_file(logger=True)
    #initial_tide_data.open(output_file)

    log_queue = Queue()

    stop_event = Event()
    lp = Process(target=listener_process, name='listener',
                 args=(log_queue, stop_event, logfile))
    lp.start()

    # The worker process configuration is just a QueueHandler attached to the
    # root logger, which allows all messages to be sent to the queue.
    # We disable existing loggers to disable the "setup" logger used in the
    # parent process. This is needed on POSIX because the logger will
    # be there in the child following a fork().
    if sys.version_info[0] < 3:
      config_worker = {
          'version': 1,
          'disable_existing_loggers': True,
          'handlers': {
              'queue': {
                  'class': 'logutils.queue.QueueHandler',
                  'queue': log_queue,
              },
          },
          'root': {
            'level': 'NOTSET',
            'handlers': ['queue']
          }
      }
    else:
      config_worker = {
          'version': 1,
          'disable_existing_loggers': True,
          'handlers': {
              'queue': {
                  'class': 'logging.handlers.QueueHandler',
                  'queue': log_queue,
              },
          },
          'root': {
            'level': 'NOTSET',
            'handlers': ['queue']
          }
      }

    logging.config.dictConfig(config_worker)
    logger = logging.getLogger()

    workers = worker_process_count
    inputQueue = Queue()
    resultQueue = Queue()
    processes = []

    if logger:
      logger.debug("Retrieving: %d tide records." % (len(date_list)))
    date_list.sort()
    for date_rec in date_list:
      inputQueue.put(date_rec)

    #Start up the worker processes.
    for workerNum in range(workers):
      args = {
        'input_queue': inputQueue,
        'results_queue': resultQueue,
        'tide_station': tide_station,
        'worker_log_config': config_worker,
        'debug_data': write_debug_file
      }
      p = Process(target=get_tide_data, kwargs=args)
      if logger:
        logger.debug("Starting process: %s" % (p._name))
      p.start()
      processes.append(p)
      inputQueue.put('STOP')


    #If we don't empty the resultQueue periodically, the .join() below would block continously.
    #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
    #the blurb on Joining processes that use queues
    rec_count = 0
    tide_recs = []
    while any([checkJob.is_alive() for checkJob in processes]):
      if not resultQueue.empty():
        tide_recs.append(resultQueue.get())
        rec_count += 1

    #Wait for the process to finish.
    for p in processes:
      p.join()

    #Poll the queue once more to get any remaining records.
    while not resultQueue.empty():
      tide_recs.append(resultQueue.get())
      rec_count += 1

    if logger:
      logger.debug("Retrieved: %d tide records" % (rec_count))

    tide_recs.sort(key=lambda rec: rec['date'], reverse=False)

    """
    tide_debug_file = None
    if write_debug_file:
      full_path, full_filename = os.path.split(output_file)
      filename, ext = os.path.splitext(full_filename)
      debug_file_name = os.path.join(full_path, "%s-tide_debug.csv" % (filename))
      tide_debug_file = open(debug_file_name, "w")
      tide_debug_file.write("Date,Orig Range,PD Range,Orig HH,Orig HH Date,PD HH,PD HH Date,Orig H,Orig H Date,PD H,PD H Date,Orig LL,Orig LL Date,PD LL,PD LL Date,Orig L,Orig L Date,PD L,PD L Date\n")
    """
    with open(output_file, "w") as tide_csv_file:
      write_header = True
      for tide_data in tide_recs:
        tide_range = ""
        tide_hi = ""
        tide_lo = ""
        hi_date = ""
        lo_date = ""
        tide_stage=""
        tide_stage_fitted = None
        if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
          try:
            tide_range = tide_data['HH']['value'] - tide_data['LL']['value']
            #Save tide station values.
            tide_hi = tide_data['HH']['value']
            hi_date = str(tide_data['HH']['date'])
            tide_lo = tide_data['LL']['value']
            lo_date = str(tide_data['LL']['date'])
            if tide_data['tide_stage'] != -9999:
              tide_stage = tide_data['tide_stage']
            if 'tide_stage_fitted' in tide_data:
              tide_stage_fitted = ""
              if tide_data['tide_stage_fitted'] != -9999:
                tide_stage_fitted = tide_data['tide_stage_fitted']
          except TypeError as e:
            if logger:
              logger.exception(e)
        else:
          if logger:
            logger.error("Tide data for station: %s date: %s not available or only partial, using Peak data." % (tide_station, tide_data['date'].strftime("%Y-%m-%dT%H:%M:%S")))
        try:
          if write_header:
            header = ["Station", "Date", "Range", "HH", "HH" "Date", "LL", "LL" "Date", "Tide Stage"]
            if tide_stage_fitted is not None:
              header.append('Tide Stage Fitted')
            tide_csv_file.write(",".join(header))
            tide_csv_file.write("\n")
            # tide_csv_file.write("Station,Date,Range,HH,HH Date,LL,LL Date,Tide Stage\n")
            write_header = False
          if tide_range is not None:
            if tide_stage_fitted is None:
              tide_csv_file.write("%s,%s,%s,%s,%s,%s,%s,%s\n"\
                   % (tide_station,
                      tide_data['date'].strftime("%Y-%m-%dT%H:%M:%S"),
                      str(tide_range),
                      str(tide_hi),hi_date,
                      str(tide_lo),lo_date,
                      tide_stage))
            else:
              tide_csv_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
                                  % (tide_station,
                                     tide_data['date'].strftime("%Y-%m-%dT%H:%M:%S"),
                                     str(tide_range),
                                     str(tide_hi), hi_date,
                                     str(tide_lo), lo_date,
                                     tide_stage,
                                     tide_stage_fitted))
        except Exception as e:
          if logger:
            logger.exception(e)
        """
        if tide_debug_file is not None:
          orig_hh = ""
          orig_hh_date = ""
          if tide_data['HH'] is not None:
            orig_hh = str(tide_data['HH']['value'])
            orig_hh_date = str(tide_data['HH']['date'])

          orig_h = ""
          orig_h_date = ""
          if tide_data['H'] is not None:
            orig_h = str(tide_data['H']['value'])
            orig_h_date = str(tide_data['H']['date'])

          orig_ll = ""
          orig_ll_date = ""
          if tide_data['LL'] is not None:
            orig_ll = str(tide_data['LL']['value'])
            orig_ll_date = str(tide_data['LL']['date'])

          orig_l = ""
          orig_l_date = ""
          if tide_data['L'] is not None:
            orig_l = str(tide_data['L']['value'])
            orig_l_date = str(tide_data['L']['date'])
            
          pda_hh = ""
          pda_hh_date = ""
          if pda_tide_data['HH'] is not None:
            pda_hh = str(pda_tide_data['HH']['value'])
            pda_hh_date = str(pda_tide_data['HH']['date'])

          pda_h = ""
          pda_h_date = ""
          if pda_tide_data['H'] is not None:
            pda_h = str(pda_tide_data['H']['value'])
            pda_h_date = str(pda_tide_data['H']['date'])

          pda_ll = ""
          pda_ll_date = ""
          if pda_tide_data['LL'] is not None:
            pda_ll = str(pda_tide_data['LL']['value'])
            pda_ll_date = str(pda_tide_data['LL']['date'])

          pda_l = ""
          pda_l_date = ""
          if pda_tide_data['L'] is not None:
            pda_l = str(pda_tide_data['L']['value'])
            pda_l_date = str(pda_tide_data['L']['date'])

          if pda_tide_data['HH'] is not None and pda_tide_data['LL'] is not None:
            pda_range = pda_tide_data['HH']['value'] - pda_tide_data['LL']['value']

          tide_debug_file.write("%s,%f,%f,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"\
               % (tide_data['date'].strftime("%Y-%m-%dT%H:%M:%S"),
                  tide_range, pda_range,
                  orig_hh, orig_hh_date,
                  pda_hh, pda_hh_date,
                  orig_h, orig_h_date,
                  pda_h, pda_h_date,
                  orig_ll, orig_ll_date,
                  pda_ll, pda_ll_date,
                  orig_l, orig_l_date,
                  pda_l, pda_l_date
                  ))
        """
    #if tide_debug_file is not None:
    #  tide_debug_file.close()
    logger.info("create_tide_data_file_mp finished.")
    logger.info("Shutting down logger process.")
    stop_event.set()
    lp.join()

  except Exception as e:
    if logger:
      logger.exception(e)

def create_tide_data_file(tide_station, date_list, output_file):
  logger = logging.getLogger(__name__)

  try:
    #Open the file and read any tide entries that exist. We'll then just try and fill in the missing
    #ones.
    #initial_tide_data = tide_data_file(logger=True)
    #initial_tide_data.open(output_file)

    with open(output_file, "w") as tide_csv_file:
      tide_csv_file.write("Station,Date,Range,Hi,Lo\n")

      for date_rec in date_list:
        if date_rec == timezone('UTC').localize(datetime.strptime('2007-05-09 04:00:00', "%Y-%m-%d %H:%M:%S")):
          i = 0

        tide = noaaTideDataExt(use_raw=True, logger=logger)

        #Date/Time format for the NOAA is YYYYMMDD, get previous 24 hours.
        wq_utc_date = date_rec.astimezone(timezone('UTC'))
        tide_start_time = (wq_utc_date - timedelta(hours=24))
        tide_end_time = wq_utc_date

        if logger:
          logger.debug("Start retrieving tide data for station: %s date: %s-%s" % (tide_station, tide_start_time, tide_end_time))
        if tide_start_time is not None:
          for x in range(0, 5):
            if logger:
              logger.debug("Attempt: %d retrieving tide data for station." % (x+1))
            tide_data = tide.calcTideRangeExt(beginDate = tide_start_time,
                               endDate = tide_end_time,
                               station=tide_station,
                               datum='MLLW',
                               units='feet',
                               timezone='GMT',
                               smoothData=False)
            if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
              try:
                tide_range = tide_data['HH']['value'] - tide_data['LL']['value']
                #Save tide station values.
                tide_hi = tide_data['HH']['value']
                tide_lo = tide_data['LL']['value']
              except TypeError as e:
                if logger:
                  logger.exception(e)
            else:
              if logger:
                logger.error("Tide data for station: %s date: %s not available or only partial, using Peak data." % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))
              try:
                tide_hi = tide_data['PeakValue']['value']
                tide_lo = tide_data['ValleyValue']['value']
                tide_range = tide_hi - tide_lo
              except TypeError as e:
                if logger:
                  logger.exception(e)

            if tide_range is not None:
              tide_csv_file.write("%s,%s,%f,%f,%f\n"\
                   % (tide_station,wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S"),tide_range,tide_hi,tide_lo))
              break

        if logger:
          logger.debug("Finished retrieving tide data for station: %s date: %s" % (tide_station, wq_utc_date.strftime("%Y-%m-%dT%H:%M:%S")))


  except IOError as e:
    if logger:
      logger.exception(e)


  return
