import sys
sys.path.append('../commonfiles')

import os

import logging.config
import optparse
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
import time
import re
from datetime import datetime, timedelta
from pytz import timezone
import requests
from multiprocessing import Process, Queue, current_process
import json


if sys.version_info[0] < 3:
  from apiclient import discovery
else:
  from googleapiclient import discovery

from oauth2client.file import Storage
from googleapiclient.http import MediaIoBaseDownload

import io

if sys.version_info[0] < 3:
  from pysqlite2 import dbapi2 as sqlite3
else:
  try:
    from pysqlite3 import dbapi2 as sqlite3
  except ModuleNotFoundError:
    import sqlite3

from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import loads as wkt_loads
from shapely.wkt import dumps as wkt_dumps
import pandas as pd
import geopandas as gpd

from pykml.factory import KML_ElementMaker as KML

from lxml import etree
from wqDatabase import wqDB

#from processXMRGFile import processXMRGData
from wqHistoricalData import item_geometry, geometry_list
from xmrgFile import xmrgFile, hrapCoord, LatLong, nexrad_db, getCollectionDateFromFilename
from geoXmrg import geoXmrg
import pickle

class xmrg_results(object):
  def __init__(self):
    self.datetime = None
    self.boundary_results = {}
    self.boundary_grids = {}

  def add_boundary_result(self, name, result_type, result_value):
    if name not in self.boundary_results:
      self.boundary_results[name] = {}

    results = self.boundary_results[name]
    results[result_type] = result_value

  def get_boundary_results(self, name):
    return(self.boundary_results[name])

  def add_grid(self, boundary_name, grid_tuple):

    if boundary_name not in self.boundary_grids:
      self.boundary_grids[boundary_name] = []

    grid_data = self.boundary_grids[boundary_name]
    grid_data.append(grid_tuple)

  def get_boundary_grid(self, boundary_name):
    grid_data = None
    if boundary_name in self.boundary_grids:
      grid_data = self.boundary_grids[boundary_name]
    return grid_data

  def get_boundary_data(self):
    if sys.version_info[0] < 3:
      for boundary_name, boundary_data in self.boundary_results.iteritems():
        yield (boundary_name, boundary_data)
    else:
      for boundary_name, boundary_data in self.boundary_results.items():
        yield (boundary_name, boundary_data)

  def get_boundary_names(self):
    return self.boundary_grids.keys()

def process_xmrg_file(**kwargs):

  try:
    try:
      processing_start_time = time.time()
      xmrg_file_count = 1
      logger = None
      if 'logger' in kwargs:
        logger_name = kwargs['logger_name']
        logger_config = kwargs['logger_config']
        #Each worker will set it's own filename for the filehandler
        base_filename = logger_config['handlers']['file_handler']['filename']
        filename_parts = os.path.split(base_filename)
        filename, ext = os.path.splitext(filename_parts[1])
        worker_filename = os.path.join(filename_parts[0], '%s_%s%s' %
                                       (filename, current_process().name.replace(':', '_'), ext))
        logger_config['handlers']['file_handler']['filename'] = worker_filename
        logging.config.dictConfig(logger_config)
        logger = logging.getLogger(logger_name)
        logger.debug("%s starting process_xmrg_file." % (current_process().name))

      inputQueue = kwargs['input_queue']
      resultsQueue = kwargs['results_queue']
      save_all_precip_vals = kwargs['save_all_precip_vals']
      #A course bounding box that restricts us to our area of interest.
      minLatLong = None
      maxLatLong = None
      if 'min_lat_lon' in kwargs and 'max_lat_lon' in kwargs:
        minLatLong = kwargs['min_lat_lon']
        maxLatLong = kwargs['max_lat_lon']

      #Boundaries we are creating the weighted averages for.
      boundaries = kwargs['boundaries']

      save_boundary_grid_cells = True
      save_boundary_grids_one_pass = True
      write_weighted_avg_debug = True

      #This is the database insert datetime.
      datetime = time.strftime( "%Y-%m-%dT%H:%M:%S", time.localtime() )

      #Create the precip database we use local to the thread.
      #nexrad_filename = "%s%s.sqlite" % (kwargs['nexrad_schema_directory'], current_process().name)
      #if os.path.isfile(nexrad_filename):
      #  os.remove(nexrad_filename)
      nexrad_db_conn = nexrad_db()
      nexrad_db_conn.connect(db_name=":memory:",
                             spatialite_lib=kwargs['spatialite_lib'],
                             nexrad_schema_files=kwargs['nexrad_schema_files'],
                             nexrad_schema_directory=kwargs['nexrad_schema_directory']
                             )
      #nexrad_db_conn.db_connection.isolation_level = None
      nexrad_db_conn.db_connection.execute("PRAGMA synchronous = OFF")
      nexrad_db_conn.db_connection.execute("PRAGMA journal_mode = MEMORY")
    except Exception as e:
      if logger:
        logger.exception(e)
      else:
        import traceback
        traceback.print_exc()


    else:
      for xmrg_filename in iter(inputQueue.get, 'STOP'):
        tot_file_time_start = time.time()
        if logger:
          logger.debug("ID: %s processing file: %s" % (current_process().name, xmrg_filename))

        xmrg_proc_obj = wqXMRGProcessing(logger=False)
        xmrg = xmrgFile(current_process().name)
        xmrg.openFile(xmrg_filename)

        # This is the database insert datetime.
        # Parse the filename to get the data time.
        (directory, filetime) = os.path.split(xmrg.fileName)
        (filetime, ext) = os.path.splitext(filetime)
        filetime = xmrg_proc_obj.getCollectionDateFromFilename(filetime)

        #Data store in hundreths of mm, we want mm, so convert.
        dataConvert = 100.0
        if xmrg.readFileHeader():
          if logger:
            logger.debug("ID: %s File Origin: X %d Y: %d Columns: %d Rows: %d" %(current_process().name, xmrg.XOR,xmrg.YOR,xmrg.MAXX,xmrg.MAXY))
          try:
            read_rows_start = time.time()
            if xmrg.readAllRows():
              if logger:
                logger.debug("ID: %s(%f secs) to read all rows in file: %s" % (current_process().name, time.time() - read_rows_start, xmrg_filename))

              #Flag to specifiy if any non 0 values were found. No need processing the weighted averages
              #below if nothing found.
              rainDataFound=False
              #If we are using a bounding box, let's get the row/col in hrap coords.
              llHrap = None
              urHrap = None
              start_col = 0
              start_row = 0
              end_col = xmrg.MAXX
              end_row = xmrg.MAXY
              if minLatLong != None and maxLatLong != None:
                llHrap = xmrg.latLongToHRAP(minLatLong, True, True)
                urHrap = xmrg.latLongToHRAP(maxLatLong, True, True)
                start_row = llHrap.row
                start_col = llHrap.column
                end_row = urHrap.row
                end_col = urHrap.column

              recsAdded = 0
              results = xmrg_results()

              #trans_cursor = nexrad_db_conn.db_connection.cursor()
              #trans_cursor.execute("BEGIN")
              add_db_rec_total_time = 0
              #for row in range(startRow,xmrg.MAXY):
              #  for col in range(startCol,xmrg.MAXX):
              for row in range(start_row, end_row):
                for col in range(start_col, end_col):
                  hrap = hrapCoord(xmrg.XOR + col, xmrg.YOR + row)
                  latlon = xmrg.hrapCoordToLatLong(hrap)
                  latlon.longitude *= -1
                  val = xmrg.grid[row][col]

                  #If there is no precipitation value, or the value is erroneous
                  if val <= 0:
                    if save_all_precip_vals:
                      val = 0
                    else:
                      continue
                  else:
                    val /= dataConvert

                  rainDataFound = True
                  #Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                  #that has each point in the grid for a given point.
                  hrapNewPt = hrapCoord( xmrg.XOR + col, xmrg.YOR + row + 1)
                  latlonUL = xmrg.hrapCoordToLatLong( hrapNewPt )
                  latlonUL.longitude *= -1

                  hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row)
                  latlonBR = xmrg.hrapCoordToLatLong( hrapNewPt )
                  latlonBR.longitude *= -1

                  hrapNewPt = hrapCoord( xmrg.XOR + col + 1, xmrg.YOR + row + 1)
                  latlonUR = xmrg.hrapCoordToLatLong( hrapNewPt )
                  latlonUR.longitude *= -1

                  grid_polygon = Polygon([(latlon.longitude, latlon.latitude),
                                          (latlonUL.longitude, latlonUL.latitude),
                                          (latlonUR.longitude, latlonUR.latitude),
                                          (latlonBR.longitude, latlonBR.latitude),
                                          (latlon.longitude, latlon.latitude)])
                  if save_boundary_grid_cells:
                    results.add_grid('complete_area', (grid_polygon, val))

                  try:
                    add_db_rec_start = time.time()
                    nexrad_db_conn.insert_precip_record(datetime, filetime,
                                                        latlon.latitude, latlon.longitude,
                                                        val,
                                                        grid_polygon,
                                                        None)
                    #if logger:
                    # logger.debug("ID: %s(%f secs insert)" % (current_process().name, time.time() - add_db_rec_start))
                    add_db_rec_total_time += time.time() - add_db_rec_start

                    recsAdded += 1
                  except Exception as e:
                    if logger:
                      logger.exception(e)
                    nexrad_db_conn.db_connection.rollback()
              #Commit the inserts.
              try:
                commit_recs_start = time.time()
                nexrad_db_conn.commit()
                commit_recs_time = time.time() - commit_recs_start
              except Exception as e:
                if logger:
                  logger.exception(e)
                nexrad_db.db_connection.rollback()
              else:
                if logger is not None:
                  logger.info("ID: %s(%f secs add %f secs commit) Processed: %d rows. Added: %d records to database."\
                              %(current_process().name, add_db_rec_total_time, commit_recs_time, (row + 1),recsAdded))

                results.datetime = filetime
                for boundary in boundaries:
                  try:
                    if save_boundary_grid_cells:
                      boundary_grid_query_start = time.time()
                      #cells_cursor = nexrad_db_conn.get_radar_data_for_boundary(boundary['polygon'], filetime, filetime)
                      cells_cursor = nexrad_db_conn.get_radar_data_for_boundary(boundary.object_geometry, filetime, filetime)
                      for row in cells_cursor:
                        cell_poly = wkt_loads(row['WKT'])
                        precip = row['precipitation']
                        #results.add_grid(boundary['name'], (cell_poly, precip))
                        results.add_grid(boundary.name, (cell_poly, precip))

                      if logger:
                        logger.debug("ID: %s(%f secs) to query grids for boundary: %s"\
                                     % (current_process().name, time.time() - boundary_grid_query_start, boundary.name))


                    avg_start_time = time.time()
                    if write_weighted_avg_debug:
                      wgtd_avg_file = os.path.join(directory, "%s_%s.csv" % (filetime.replace(':', '_'), boundary.name.replace(' ', '_')))
                    #avg = nexrad_db_conn.calculate_weighted_average(boundary['polygon'], filetime, filetime)
                    avg = nexrad_db_conn.calculate_weighted_average(boundary.object_geometry, filetime, filetime, wgtd_avg_file)
                    #results.add_boundary_result(boundary['name'], 'weighted_average', avg)
                    results.add_boundary_result(boundary.name, 'weighted_average', avg)
                    avg_total_time = time.time() - avg_start_time
                    if logger:
                      logger.debug("ID: %s(%f secs) to process average for boundary: %s"\
                                   % (current_process().name, avg_total_time, boundary.name))
                  except Exception as e:
                    if logger:
                      logger.exception(e)
              resultsQueue.put(results)

              nexrad_db_conn.delete_all()
            #Only do it for one file. Following files should all be same results other than the precip values.
            if save_boundary_grids_one_pass:
              save_boundary_grid_cells = False
            xmrg.cleanUp(kwargs['delete_source_file'], kwargs['delete_compressed_source_file'])
            xmrg.Reset()
            #Counter for number of files processed.
            xmrg_file_count += 1
            if logger:
              logger.debug("ID: %s(%f secs) total time to process data for file: %s" % (current_process().name, time.time() - tot_file_time_start, xmrg_filename))
          except Exception as e:
            if logger:
              logger.exception(e)

        else:
          if logger:
            logger.error("Process: %s Failed to process file: %s" % (current_process().name, xmrg_filename))

      if nexrad_db_conn:
        nexrad_db_conn.close()

      if logger:
        logger.debug("ID: %s process finished. Processed: %d files in time: %f seconds"\
                     % (current_process().name, xmrg_file_count, time.time() - processing_start_time))
  except Exception as e:
    logger.exception(e)
  return


"""
Want to move away form the XML config file used and use an ini file. Create a new class
inheritting from the dhec one.
"""
class wqXMRGProcessing(object):
  def __init__(self, logger=True, logger_name='nexrad_mp_logging', logger_config=None):

    self.logger = None
    self.logger_name = logger_name
    self.logger_config = logger_config
    if logger:
      #self.logger = logging.getLogger(type(self).__name__)
      self.logger = logging.getLogger(logger_name)
    self.xenia_db = None
    self.boundaries = geometry_list(use_logger=True) #[]
    self.sensor_ids = {}
    self.kml_time_series = None
    try:
      #2011-07-25 DWR
      #Added a processing start time to use for the row_entry_date value when we add new records to the database.
      self.processingStartTime = time.strftime('%Y-%d-%m %H:%M:%S', time.localtime())
      self.configSettings = None

    except (ConfigParser.Error, Exception) as e:
      if self.logger is not None:
        self.logger.exception(e)
      pass

  def load_config_settings(self, **kwargs):
    #self.configSettings = configSettings(kwargs['config_file'])
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(kwargs['config_file'])

      bbox = configFile.get('nexrad_database', 'bbox')
      self.minLL = None
      self.maxLL = None
      if(bbox != None):
        latLongs = bbox.split(';')
        self.minLL = LatLong()
        self.maxLL = LatLong()
        latlon = latLongs[0].split(',')
        self.minLL.latitude = float( latlon[0] )
        self.minLL.longitude = float( latlon[1] )
        latlon = latLongs[1].split(',')
        self.maxLL.latitude = float( latlon[0] )
        self.maxLL.longitude = float( latlon[1] )

      self.writePrecipToKML = configFile.getboolean('nexrad_database', 'writeToKML')
      try:
        self.kmlColorsFile = configFile.get('nexrad_database', 'kmlColors')
      except ConfigParser.Error as e:
        self.kmlColorsFile = None
      try:
        self.kmlTimeSeries = configFile.getboolean('nexrad_database', 'kmlCreateTimeSeries')
        self.kml_time_series = []
      except ConfigParser.Error as e:
        self.kmlTimeSeries = False

      #If we are going to write shapefiles, get the output directory.
      if(self.writePrecipToKML):
        self.KMLDir = configFile.get('nexrad_database', 'KMLDir')
        if(len(self.KMLDir) == 0):
          self.writePrecipToKML = 0
          if self.logger is not None:
            self.logger.error("No KML directory provided, will not write shapefiles.")

      self.saveAllPrecipVals = configFile.getboolean('nexrad_database', 'saveAllPrecipVals')

      #self.createPolygonsFromGrid = configFile.getboolean('nexrad_database', 'createPolygonsFromGrid')

      #Flag to specify if we want to delete the compressed XMRG file when we are done processing.
      #We might not be working off a compressed source file, so this flag only applies to a compressed file.
      self.deleteCompressedSourceFile = configFile.getboolean('nexrad_database', 'deleteCompressedSourceFile')

      #Flag to specify if we want to delete the XMRG file when we are done processing.
      self.deleteSourceFile = configFile.getboolean('nexrad_database', 'deleteSourceFile')

      #Flag to specify if we want to calculate the weighted averages for the watersheds as we write the radar data
      #into the precipitation_radar table.
      self.calcWeightedAvg =configFile.getboolean('nexrad_database', 'calculateWeightedAverage')


      self.dbName = configFile.get('database', 'name')
      self.spatiaLiteLib = configFile.get('database', 'spatiaLiteLib')

      self.baseURL = configFile.get('nexrad_database', 'baseURL')
      #This tag is used to help further refine the files we process. For instance, hourly xmrg files are prepended
      #with xmrg whereas the 6hr and 24hr files aren't. So we could use this to ignore those.
      #self.fileNameFilter = configFile.get('nexrad_database', 'fileNameFilter')
      self.xmrgDLDir = configFile.get('nexrad_database', 'downloadDir')

      #Directory where the NEXRAD database schema files live.
      self.nexrad_schema_directory = configFile.get('nexrad_database', 'schema_directory')
      #The files that create the tables we need in our NEXRAD DB.
      self.nexrad_schema_files = configFile.get('nexrad_database', 'schema_files').split(',')

      #File containing the boundaries we want to use to carve out data from the XMRG file.
      self.boundaries_file = configFile.get('boundaries_settings', 'boundaries_file')

      #Number of worker processes to start.
      self.worker_process_count = configFile.getint('nexrad_database', 'worker_process_count')

      #Specifies to attempt to add the sensors before inserting the data. Only need to do this
      #on intial run.
      self.add_sensors = True
      #Specifies to attempt to add the platforms representing the radar coverage.
      self.add_platforms = True

      self.save_boundary_grid_cells = True
      self.save_boundary_grids_one_pass = True

    except (ConfigParser.Error, Exception) as e:
      if self.logger:
        self.logger.exception(e)

    #Default extension to use for XMRG file when we are building the name
    self.xmrg_file_ext = '.gz'
    try:
      self.xmrg_file_ext = configFile.get('nexrad_database', 'xmrg_file_ext')
    except (ConfigParser.Error, Exception) as e:
      if self.logger:
        self.logger.error("No XMRG file extension given, defaultin to: %s" % (self.xmrg_file_ext))
        self.logger.exception(e)

    self.use_http_file_pull = True
    #See if we are getting from sftp site and not a web accessible directory.
    try:

      self.sftp = False
      self.sftp_user = None
      self.sftp_password = None
      self.sftp = configFile.getboolean('nexrad_database', 'use_sftp')
      if self.logger:
        self.logger.debug("Use sftp: %s" % (self.sftp))
      self.sftp_base_directory = configFile.get('nexrad_database', 'sftp_base_directory')
      if self.sftp:
        self.use_http_file_pull = False

        pwd_file = configFile.get('nexrad_database', 'sftp_password_file')
        pwd_config_file = ConfigParser.RawConfigParser()
        pwd_config_file.read(pwd_file)
        self.sftp_user = pwd_config_file.get('nexrad_sftp', 'user')
        self.sftp_password = pwd_config_file.get('nexrad_sftp', 'password')

    except (ConfigParser.Error, Exception) as e:
      if self.logger:
        self.logger.exception(e)

    #Check to see if we're getting files from google drive
    try:
      self.use_google_drive = False
      self.use_google_drive = configFile.getboolean('nexrad_database', 'use_google_drive')
      if self.use_google_drive:
        self.use_http_file_pull = False
        self.logger.debug("Downloading from google drive.")
        google_setup_file = configFile.get('nexrad_database', 'google_setup_file')
        google_cfg_file = ConfigParser.RawConfigParser()
        google_cfg_file.read(google_setup_file)
        self.google_credentials_json = google_cfg_file.get('google_drive', 'credentials_file')
        self.google_folder_id = google_cfg_file.get('google_drive', 'xmrg_folder_id')
        self.logger.debug("Google folder id: %s Credentials file: %s" % (self.google_folder_id, self.google_credentials_json))
    except (ConfigParser.Error, Exception) as e:
      if self.logger:
        self.logger.exception(e)

    #Process the boundaries
    try:
      header_row = ["WKT", "NAME"]
      if self.logger:
        self.logger.debug("Reading boundaries geometry file: %s" % (self.boundaries_file))

      self.boundaries.load(self.boundaries_file)

      #Create the connection to the xenia database where our final results are stored.
      self.xenia_db = wqDB(self.dbName, type(self).__name__)
      if self.add_platforms:
        org_id = self.xenia_db.organizationExists('nws')
        if org_id == -1:
          org_id =  self.xenia_db.addOrganization({'short_name': 'nws'})
        #Add the platforms to represent the watersheds and drainage basins
        for boundary in self.boundaries:
          #platform_handle = 'nws.%s.radarcoverage' % (boundary['name'])
          platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
          if self.xenia_db.platformExists(platform_handle) == -1:
            if self.logger:
              self.logger.debug("Adding platform. Org: %d Platform Handle: %s Short_Name: %s"\
                                % (org_id, platform_handle, boundary.name))
            if self.xenia_db.addPlatform({'organization_id': org_id,
                                          'platform_handle': platform_handle,
                                          'short_name': boundary.name, #'short_name': boundary['name'],
                                          'active': 1}) == -1:
              self.logger.error("Failed to add platform: %s for org_id: %d, cannot continue" % (platform_handle, org_id))

    except (IOError,Exception) as e:
      if self.logger:
        self.logger.exception(e)


  def getCollectionDateFromFilename(self, fileName):
    #Parse the filename to get the data time.
    (directory,filetime) = os.path.split( fileName )
    (filetime,ext) = os.path.splitext( filetime )
    #Let's get rid of the xmrg verbage so we have the time remaining.
    #The format for the time on these files is MMDDYYY sometimes a trailing z or for some historical
    #files, the format is xmrg_MMDDYYYY_HRz_SE. The SE could be different for different regions, SE is southeast.     
    #24 hour files don't have the z, or an hour
    
    dateformat = "%m%d%Y%H" 
    #Regexp to see if we have one of the older filename formats like xmrg_MMDDYYYY_HRz_SE
    fileParts = re.findall("xmrg_\d{8}_\d{1,2}", filetime)
    if len(fileParts):
      #Now let's manipulate the string to match the dateformat var above.
      filetime = re.sub("xmrg_", "", fileParts[0])
      filetime = re.sub("_","", filetime)
    else:
      if filetime.find('24hrxmrg') != -1:
        dateformat = "%m%d%Y"
      filetime = filetime.replace('24hrxmrg', '')
      filetime = filetime.replace('xmrg', '')
      filetime = filetime.replace('z', '')
    #Using mktime() and localtime() is a hack. The time package in python doesn't have a way
    #to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
    #I want instead of brining in the calender package which has the conversion.
    secs = time.mktime(time.strptime(filetime, dateformat))
    #secs -= offset
    filetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(secs))
    
    return(filetime)

  def write_kml_time_series(self):
    start_kml_time = time.time()
    if self.logger:
      self.logger.info("Start write_kml_time_series")
    try:
        with open(self.kmlColorsFile, 'r') as color_file:
          kml_colors_list = json.load(color_file)
          styles = []
          kml_docu = KML.Document(
            #KML.Name("Boundary: %s" % (boundary))
          )

          for ndx,color in enumerate(kml_colors_list['limits']):
              if color['high'] is not None:
                color['high'] = color['high'] * 25.4
              if color['low'] is not None:
                color['low'] = color['low'] * 25.4
              #Colors are in HTML syntax, convert to KML
              if ndx == 0:
                opacity = '20'
              else:
                opacity = 'ff'
              color_val = "%s%s%s%s" % (opacity, color['color'][4:6], color['color'][2:4], color['color'][0:2])
              color['color'] = color_val
              kml_docu.append(KML.Style(
                KML.LineStyle(
                    KML.color(color['color']),
                    KML.width(3),
                ),
                KML.PolyStyle(
                    KML.color(color['color']),
                ),
                id='style_%d' % (ndx)
            ))
          kml_doc = KML.kml(kml_docu)
    except Exception as e:
        self.logger.exception(e)
    #doc = etree.SubElement(kml_doc, 'Document')
    try:
      """
      self.kml_time_series['results'].append({'datetime': xmrg_results['datetime'],
                                        'boundary_results': xmrg_results['boundary_results']})
      """
      #Sort the results based on datetime
      self.kml_time_series.sort(key=lambda result: result.datetime)
      boundary_names = self.kml_time_series[0].get_boundary_names()
      for boundary in boundary_names:
        # Get list of the grids
        for results in self.kml_time_series:

            kml_docu.append(KML.Name("Boundary: %s" % (boundary)))
            date_time = results.datetime
            boundary_grids = results.get_boundary_grid(boundary)

            for polygon, val in boundary_grids:
              coords = " ".join("%s,%s,0" % (tup[0],tup[1]) for tup in polygon.exterior.coords[:])
              if self.kmlColorsFile is not None:
                  for ndx,color in enumerate(kml_colors_list['limits']):
                    if val  >= color['low'] and val < color['high']:
                        style_id = "#style_%d" % (ndx)
                        break
              else:
                style_id = 'grid_style'
              kml_doc.Document.append(KML.Placemark(KML.name('%f' % val),
                                                    KML.styleUrl(style_id),
                                                    KML.TimeStamp(KML.when(date_time)),
                                                     KML.Polygon(
                                                       KML.outerBoundaryIs(
                                                         KML.LinearRing(
                                                          KML.coordinates(coords)
                                                         )
                                                       )
                                                     ))
              )
        try:
          kml_outfile = os.path.join(self.KMLDir, "%s_%s.kml" % (boundary, date_time.replace(':', '_')))
          if self.logger:
            self.logger.debug("write_boundary_grid_kml KML outfile: %s" % (kml_outfile))
          kml_file = open(kml_outfile, "w")
          kml_file.write(etree.tostring(kml_doc, pretty_print=True))
          kml_file.close()
        except (IOError,Exception) as e:
          if self.logger:
            self.logger.exception(e)

    except (IOError, Exception) as e:
      if self.logger:
        self.logger.exception(e)

    if self.logger:
        self.logger.info("End write_kml_time_series in %f seconds" % (time.time()-start_kml_time))
    return

  #def write_boundary_grid_kml(self, boundary, datetime, boundary_grids):
  def write_boundary_grid_kml(self, boundary, results, build_time_series=False):
    date_time = results.datetime
    boundary_grids = results.get_boundary_grid(boundary)
    if self.logger:
      self.logger.info("Start write_boundary_grid_kml for boundary: %s Date: %s" % (boundary, date_time))
    if self.kmlColorsFile is None:
        kml_doc = KML.kml(KML.Document(
                            KML.Name("Boundary: %s" % (boundary)),
                            KML.Style(
                              KML.LineStyle(
                                KML.color('ffff0000'),
                                KML.width(3),
                              ),
                              KML.PolyStyle(
                                KML.color('800080ff'),
                              ),
                              id='grid_style'
                            )
                          )
        )
    else:
      try:
          with open(self.kmlColorsFile, 'r') as color_file:
            kml_colors_list = json.load(color_file)
            styles = []
            kml_docu = KML.Document(
              KML.Name("Boundary: %s" % (boundary))
            )

            for ndx,color in enumerate(kml_colors_list['limits']):
                if color['high'] is not None:
                  color['high'] = color['high'] * 25.4
                if color['low'] is not None:
                  color['low'] = color['low'] * 25.4
                #Colors are in HTML syntax, convert to KML
                if ndx == 0:
                  opacity = '20'
                else:
                  opacity = 'ff'
                color_val = "%s%s%s%s" % (opacity, color['color'][4:6], color['color'][2:4], color['color'][0:2])
                color['color'] = color_val
                kml_docu.append(KML.Style(
                  KML.LineStyle(
                      KML.color(color['color']),
                      KML.width(3),
                  ),
                  KML.PolyStyle(
                      KML.color(color['color']),
                  ),
                  id='style_%d' % (ndx)
              ))
            kml_doc = KML.kml(kml_docu)
      except Exception as e:
          self.logger.exception(e)
    #doc = etree.SubElement(kml_doc, 'Document')
    try:
      for polygon, val in boundary_grids:
        coords = " ".join("%s,%s,0" % (tup[0],tup[1]) for tup in polygon.exterior.coords[:])
        if self.kmlColorsFile is not None:
            for ndx,color in enumerate(kml_colors_list['limits']):
              if val  >= color['low'] and val < color['high']:
                  style_id = "#style_%d" % (ndx)
                  break
        else:
          style_id = 'grid_style'
        kml_doc.Document.append(KML.Placemark(KML.name('%f' % val),
                                              KML.styleUrl(style_id),
                                              KML.TimeStamp(KML.when(date_time)),
                                               KML.Polygon(
                                                 KML.outerBoundaryIs(
                                                   KML.LinearRing(
                                                    KML.coordinates(coords)
                                                   )
                                                 )
                                               ))
        )
    except (TypeError,Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        kml_outfile = os.path.join(self.KMLDir, "%s_%s.kml" % (boundary, date_time.replace(':', '_')))
        #kml_outfile = "%s%s_%s.kml" % (self.KMLDir, boundary, date_time.replace(':', '_'))
        if self.logger:
          self.logger.debug("write_boundary_grid_kml KML outfile: %s" % (kml_outfile))
        kml_file = open(kml_outfile, "w")
        kml_file.write(etree.tostring(kml_doc, pretty_print=True).decode('UTF-8'))
        kml_file.close()
      except (IOError,Exception) as e:
        if self.logger:
          self.logger.exception(e)

    if self.logger:
      self.logger.info("End write_boundary_grid_kml for boundary: %s Date: %s" % (boundary, date_time))
    return

  def import_files(self, file_list):
    if self.logger:
      self.logger.debug("Start import_files" )

      workers = self.worker_process_count
      inputQueue = Queue()
      resultQueue = Queue()
      processes = []

      if self.logger:
        self.logger.debug("Importing: %d files." % (len(file_list)))
      for file_name in file_list:
        inputQueue.put(file_name)

      sqlite3.connect(":memory:").close()

      #Start up the worker processes.
      for workerNum in range(workers):
        args = {
          'logger': True,
          'logger_name': self.logger_name,
          'logger_config': self.logger_config,
          'input_queue': inputQueue,
          'results_queue': resultQueue,
          'min_lat_lon': self.minLL,
          'max_lat_lon': self.maxLL,
          'nexrad_schema_files': self.nexrad_schema_files,
          'nexrad_schema_directory':self.nexrad_schema_directory,
          'save_all_precip_vals': self.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.spatiaLiteLib,
          'delete_source_file': self.deleteSourceFile,
          'delete_compressed_source_file': self.deleteCompressedSourceFile
        }
        p = Process(target=process_xmrg_file, kwargs=args)
        if self.logger:
          self.logger.debug("Starting process: %s" % (p._name))
        p.start()
        processes.append(p)
        inputQueue.put('STOP')


      #If we don't empty the resultQueue periodically, the .join() below would block continously.
      #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
      #the blurb on Joining processes that use queues
      '''
      self.logger.debug("Begin checking Queue for results")
      process_queues = True
      while process_queues:
        for checkJob in processes:
          if (rec_count % 10) == 0:
            self.logger.debug("Processed %d results" % (rec_count))
          if checkJob is not None and checkJob.is_alive():
            if not resultQueue.empty():
              self.process_result(resultQueue.get())
              rec_count += 1
      '''

      rec_count = 0
      self.logger.debug("Waiting for %d processes to complete" % (workers))
      while any([(checkJob is not None and checkJob.is_alive()) for checkJob in processes]):
        if not resultQueue.empty():
          #finalResults.append(resultQueue.get())
          if (rec_count % 10) == 0:
            self.logger.debug("Processed %d results" % (rec_count))
          self.process_result(resultQueue.get())
          rec_count += 1

      '''
      #Wait for the process to finish.
      self.logger.debug("Waiting for %d processes to complete" % (workers))
      for p in processes:
        self.logger.debug("Waiting for process: %s to complete" % (p._name))
        if p.is_alive():
          p.join()
        else:
          self.logger.debug("Process: %s already completed" % (p._name))
      '''
      #Poll the queue once more to get any remaining records.
      while not resultQueue.empty():
        self.logger.debug("Pulling records from resultsQueue.")
        self.process_result(resultQueue.get())
        rec_count += 1

      if self.logger:
        self.logger.debug("Imported: %d records" % (rec_count))

    if self.kmlTimeSeries:
      self.write_kml_time_series()

    if self.logger:
      self.logger.debug("Finished import_files" )

    return

  def importFiles(self, importDirectory=None):
    try:
      if importDirectory is None:
        importDirectory = self.importDirectory

      if self.logger:
        self.logger.debug("Importing from: %s" % (importDirectory))

      workers = self.worker_process_count
      inputQueue = Queue()
      resultQueue = Queue()
      finalResults = []
      processes = []

      fileList = os.listdir(importDirectory)
      fileList.sort()
      #If we want to skip certain months, let's pull those files out of the list.
      monthList = {'Jan': 1, 'Feb': 2, 'Mar': 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12 }
      #startMonth = monthList[startMonth]
      #endMonth = monthList[endMonth]
      for file_name in fileList:
        full_path = "%s%s" % (importDirectory, file_name)
        inputQueue.put(full_path)

      #Start up the worker processes.
      for workerNum in xrange(workers):
        args = {
          'logger': True,
          'logger_name': self.logger_name,
          'logger_config': self.logger_config,
          'input_queue': inputQueue,
          'results_queue': resultQueue,
          'min_lat_lon': self.minLL,
          'max_lat_lon': self.maxLL,
          'nexrad_schema_files': self.nexrad_schema_files,
          'nexrad_schema_directory':self.nexrad_schema_directory,
          'save_all_precip_vals': self.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.spatiaLiteLib
        }
        p = Process(target=process_xmrg_file, kwargs=args)
        if self.logger:
          self.logger.debug("Starting process: %s" % (p._name))
        p.start()
        processes.append(p)
        inputQueue.put('STOP')


      #If we don't empty the resultQueue periodically, the .join() below would block continously.
      #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
      #the blurb on Joining processes that use queues
      rec_count = 0
      while any([checkJob.is_alive() for checkJob in processes]):
        if(resultQueue.empty() == False):

          #finalResults.append(resultQueue.get())
          self.process_result(resultQueue.get())
          rec_count += 1

      #Wait for the process to finish.
      for p in processes:
        p.join()

      #Poll the queue once more to get any remaining records.
      while(resultQueue.empty() == False):
        self.process_result(resultQueue.get())
        rec_count += 1

      if self.logger:
        self.logger.debug("Finished. Import: %d records from: %s" % (rec_count, importDirectory))

      if self.kmlTimeSeries:
        self.write_kml_time_series()

    except Exception as E:
      self.lastErrorMsg = str(E)
      if self.logger is not None:
        self.logger.exception(E)

  def process_result(self, xmrg_results_data):
    try:
      if self.writePrecipToKML:
        if self.writePrecipToKML and xmrg_results_data.get_boundary_grid('complete_area') is not None:
          #self.write_boundary_grid_kml('complete_area', xmrg_results_data.datetime, xmrg_results_data.get_boundary_grid('complete_area'))
          self.write_boundary_grid_kml('complete_area', xmrg_results_data)

      for boundary_name, boundary_results in xmrg_results_data.get_boundary_data():
        if self.writePrecipToKML and xmrg_results_data.get_boundary_grid(boundary_name) is not None:
          #self.write_boundary_grid_kml(boundary_name, xmrg_results_data.datetime, xmrg_results_data.get_boundary_grid(boundary_name))
          self.write_boundary_grid_kml(boundary_name, xmrg_results_data)

        if self.kmlTimeSeries:
          self.kml_time_series.append(xmrg_results_data)

        platform_handle = "nws.%s.radarcoverage" % (boundary_name)
        lat = 0.0
        lon = 0.0

        avg = boundary_results['weighted_average']
        #self.save_data()
        if avg != None:
          if avg > 0.0 or self.saveAllPrecipVals:
            if avg != -9999:
              mVals = []
              mVals.append(avg)
              if self.add_sensors:
                self.xenia_db.addSensor('precipitation_radar_weighted_average', 'mm',
                                        platform_handle,
                                        1,
                                        0,
                                        1, None, True)
              #Build a dict of m_type and sensor_id for each platform to make the inserts
              #quicker.
              if platform_handle not in self.sensor_ids:
                m_type_id = self.xenia_db.getMTypeFromObsName('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                sensor_id = self.xenia_db.sensorExists('precipitation_radar_weighted_average', 'mm', platform_handle, 1)
                self.sensor_ids[platform_handle] = {
                  'm_type_id': m_type_id,
                  'sensor_id': sensor_id}


              #Add the avg into the multi obs table. Since we are going to deal with the hourly data for the radar and use
              #weighted averages, instead of keeping lots of radar data in the radar table, we calc the avg and
              #store it as an obs in the multi-obs table.
              add_obs_start_time = time.time()
              try:
                if self.xenia_db.addMeasurementWithMType(self.sensor_ids[platform_handle]['m_type_id'],
                                                self.sensor_ids[platform_handle]['sensor_id'],
                                                platform_handle,
                                                xmrg_results_data.datetime,
                                                lat, lon,
                                                0,
                                                mVals,
                                                1,
                                                True,
                                                self.processingStartTime):
                  if self.logger is not None:
                    self.logger.debug("Platform: %s Date: %s added weighted avg: %f in %f seconds." %(platform_handle, xmrg_results.datetime, avg, time.time() - add_obs_start_time))
              except sqlite3.IntegrityError:
                # sql = 'UPDATE multi_obs SET(m_value=%f) WHERE m_type_id=%d AND sensor_id=%d AND m_date=date' % ()
                try:
                  add_obs_start_time = time.time()
                  if self.xenia_db.updateMeasurement(self.sensor_ids[platform_handle]['m_type_id'],
                                                  self.sensor_ids[platform_handle]['sensor_id'],
                                                  platform_handle,
                                                  xmrg_results_data.datetime,
                                                  mVals):
                    self.logger.debug("Platform: %s Date: %s updated weighted avg: %f in %f seconds." %(platform_handle, xmrg_results_data.datetime, avg, time.time() - add_obs_start_time))

                except Exception as e:
                  self.logger.exception(e)
              except Exception as e:
                self.logger.exception(e)

            else:
              if self.logger is not None:
                self.logger.debug( "Platform: %s Date: %s weighted avg: %f(mm) is not valid, not adding to database." %(platform_handle, xmrg_results_data.datetime, avg))
          else:
            if self.logger is not None:
              self.logger.debug( "Platform: %s Date: %s configuration parameter not set to add precip values of 0.0." %(platform_handle, xmrg_results_data.datetime))
        else:
          if self.logger is not None:
            self.logger.error( "Platform: %s Date: %s Weighted AVG error: %s" %(platform_handle, xmrg_results_data.datetime, self.xenia_db.getErrorInfo()) )
            self.xenia_db.clearErrorInfo()
      if self.save_boundary_grids_one_pass:
        self.writePrecipToKML = False

    except StopIteration as e:
      if self.logger:
        self.logger.info("Date: %s Boundary data exhausted" % (xmrg_results_data.datetime))

    return

  def save_data(self):
    return

  def http_download_file(self, file_name):
    start_time = time.time()
    remote_filename_url = os.path.join(self.baseURL, file_name)
    if self.logger:
      self.logger.debug("Downloading file: %s" % (remote_filename_url))
    try:
      r = requests.get(remote_filename_url, stream=True)
    except (requests.HTTPError, requests.ConnectionError, Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      if r.status_code == 200:
        dest_file = os.path.join(self.xmrgDLDir, file_name)
        if self.logger:
          self.logger.debug("Saving to file: %s" % (dest_file))
        try:
          with open(dest_file, 'wb') as xmrg_file:
            for chunk in r:
              xmrg_file.write(chunk)
            if self.logger:
              self.logger.debug("Downloaded file: %s in %f seconds." % (dest_file, time.time()-start_time))
        except IOError as e:
          if self.logger:
            self.logger.exception(e)
        return dest_file
      else:
        if self.logger:
          self.logger.error("Unable to download file: %s" % (remote_filename_url))
    return None

  def google_drive_download_file(self, **kwargs):
    file_path = None
    xmrg_file_name = kwargs['file_name']
    for file_info in kwargs['google_file_list']:
      if file_info['name'] == xmrg_file_name:
        start_time = time.time()
        print("Google Drive Downloading file: %s" % (file_info['name']))
        file_path = os.path.join(self.xmrgDLDir, file_info['name'])
        file_res = self.google_drive.files().get_media(fileId=file_info['id'])
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, file_res)
        done = False
        while done is False:
          status, done = downloader.next_chunk()

        self.logger.debug("Google Drive file: %s dl'd in %f seconds." % (file_path, time.time() - start_time))
        break


    return file_path
  def sftp_download_file(self, **kwargs):
    start_time = time.time()

    try:
      file_name = kwargs['file_name']
      remote_file = os.path.join(self.sftp_base_directory, file_name)
      dest_file = os.path.join(self.xmrgDLDir, file_name)
      if self.logger:
        self.logger.debug("FTPing file: %s" % (remote_file))

      ftp = kwargs['ftp_obj']
      ftp.get(remote_file, dest_file)
      if self.logger:
        self.logger.debug("FTPd file: %s in %f seconds." % (dest_file, time.time()-start_time))

      return  dest_file
    except (IOError, Exception) as e:
      if self.logger:
        self.logger.exception(e)
    return None
  def download_file_list(self, file_list):
    files_downloaded = []
    if self.sftp:
      import paramiko

      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect(self.baseURL, username=self.sftp_user, password=self.sftp_password)
      ftp = ssh.open_sftp()
    elif self.use_google_drive:
      '''
      store = Storage(self.google_credentials_json)
      google_drive_credentials = store.get()
      http = google_drive_credentials.authorize(httplib2.Http())
      '''
      # The file token.pickle stores the user's access and refresh tokens, and is
      # created automatically when the authorization flow completes for the first
      # time.
      try:
        self.logger.debug("Opening google credential file: %s" % (self.google_credentials_json))
        if os.path.exists(self.google_credentials_json):
          with open(self.google_credentials_json, 'rb') as token:
            creds = pickle.load(token)

          self.google_drive = discovery.build('drive', 'v3', credentials=creds)

          #Check if we have to keep requesting pages to get all files.
          pageToken = ''
          get_next_page = True
          google_drive_file_list = []
          while get_next_page == True:
            self.logger.debug("Preparing to list files in directory.")
            google_req = self.google_drive.files().list(q="'%s' in parents" % (self.google_folder_id),pageSize=100, pageToken=pageToken).execute()
            google_drive_file_list.extend(google_req.get('files'))
            self.logger.debug("Google Drive folder file list query returned: %d recs" % (len(google_drive_file_list)))
            if 'nextPageToken' in google_req and len(google_req['nextPageToken']):
              self.logger.debug("Google Drive reports more files to pull.")
              pageToken = google_req['nextPageToken']
            else:
              get_next_page = False
        else:
          self.logger.error("Cannot open the google credentials file: %s" % (self.google_credentials_json))
      except Exception as e:
        self.logger.exception(e)
    for file_name in file_list:
      if self.use_http_file_pull:
        dl_filename = self.http_download_file(file_name)
      else:
        if self.sftp:
          try:
            dl_filename = self.sftp_download_file(file_name=file_name,
                                                  ftp_obj=ftp)
          except Exception as e:
            if self.logger:
              self.logger.exception(e)
        elif self.use_google_drive:
          self.logger.debug("Preparing to download from google drive.")
          dl_filename = self.google_drive_download_file(file_name=file_name,
                                                        google_file_list=google_drive_file_list)
      if dl_filename is not None:
        files_downloaded.append(dl_filename)

    return files_downloaded

  def download_range(self, start_date, hour_count):
    files_downloaded = []
    if self.logger:
      self.logger.debug("Starting download_range")
    try:
      file_list = self.file_list_from_date_range(start_date, hour_count)
      self.logger.debug("File list has: %d files" % (len(file_list)))
    except ConfigParser.Error as  e:
      if self.logger:
        self.logger.exception(e)
    else:
      self.logger.debug("Preparing to download files")
      files_downloaded = self.download_file_list(file_list)
    if self.logger:
      self.logger.debug("Finished download_range")

    return files_downloaded
  """
  Function: file_list_from_date_range
  Purpose: Given the starting date and the number of hours in the past, this builds a list
   of the xmrg filenames.
  Parameters:
    start_date_time: A datetime object representing the starting time.
    hour_count: An integer for the number of previous hours we want to build file names for.
  Return:
    A list containing the filenames.
  """
  def file_list_from_date_range(self, start_date_time, hour_count):
    file_list = []
    for x in range(hour_count):
      hr = x + 1
      date_time = start_date_time - timedelta(hours=hr)
      file_name = self.build_filename(date_time)

      file_list.append(file_name)

    return file_list

  def build_filename(self, date_time):
      file_name = date_time.strftime('xmrg%m%d%Y%Hz')
      if self.xmrg_file_ext:
        file_name += '.' + self.xmrg_file_ext
      return file_name

  def fill_gaps(self, start_date_time, hour_count):
    if self.logger:
      self.logger.debug("Starting fill_gaps for start time: %s Previous Hours: %d" % (start_date_time, hour_count))
    time_list = []
    #Build list of times that make up the range.
    for x in range(hour_count):
      hr = x + 1
      date_time = start_date_time - timedelta(hours=hr)
      #time_list.append(date_time.strftime('%Y-%m-%dT%H:%M:%S'))
      time_list.append(date_time)

    begin_date = start_date_time - timedelta(hours=hour_count)
    end_date = start_date_time
    boundary_missing_times = {}
    for boundary in self.boundaries:
      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      boundary_missing_times[boundary.name] = time_list[:]
      try:
        sql = "SELECT m_date FROM multi_obs WHERE m_date >= '%s' AND m_date < '%s' AND platform_handle='%s' ORDER BY m_date"\
        % (begin_date.strftime('%Y-%m-%dT%H:%M:%S'), end_date.strftime('%Y-%m-%dT%H:%M:%S'), platform_handle)
        dbCursor = self.xenia_db.DB.cursor()
        dbCursor.execute( sql )
        for row in dbCursor:
          db_datetime = timezone('UTC').localize(datetime.strptime(row['m_date'], '%Y-%m-%dT%H:%M:%S'))
          if db_datetime in time_list:
            boundary_missing_times[boundary.name].remove(db_datetime)

        if self.logger:
          self.logger.debug("Boundary: %s needs %d files to fill gap." % (boundary.name, len(boundary_missing_times[boundary.name])))
        dbCursor.close()
      except Exception as e:
        if self.logger:
          self.logger.exception(e)
    #Now build a non duplicate time list.
    date_time_unions = []
    for boundary_name in boundary_missing_times:
      date_time_unions = list(set(date_time_unions) | set(boundary_missing_times[boundary_name]))

    date_time_unions.sort()
    if self.logger:
      self.logger.debug("Times missing: %s" % (",".join([dts.strftime("%Y-%m-%dT%H:%M:%S") for dts in date_time_unions])))

    dl_file_list = [self.build_filename(date_time) for date_time in date_time_unions]

    if self.logger:
      self.logger.debug("Files to D/L: %s" % (str(dl_file_list)))

    files_downloaded = self.download_file_list(dl_file_list)

    self.import_files(files_downloaded)
    if self.logger:
      self.logger.debug("Finished fill_gaps for start time: %s Previous Hours: %d" % (start_date_time, hour_count))

    return
  """
  Function: vacuumDB
  Purpose: Frees up unused space in the database.
  """
  def vacuumDB(self):

    retVal = False
    if self.logger is not None:
      stats = os.stat(self.dbName)
      self.logger.debug("Begin database vacuum. File size: %d" % (stats[os.stat.ST_SIZE]))
    db = wqDB(self.dbSettings.dbName, None, self.logger)
    if(db.vacuumDB() != None):
      if self.logger is not None:
        stats = os.stat(self.dbSettings.dbName)
        self.logger.debug("Database vacuum completed. File size: %d" % (stats[os.stat.ST_SIZE]))
      retVal = True
    else:
      self.logger.error("Database vacuum failed: %s" % (db.lastErrorMsg))
      db.lastErrorMsg = ""
    db.DB.close()
    return(retVal)

'''
'''


def process_xmrg_file_geopandas(**kwargs):
  try:
    try:
      processing_start_time = time.time()
      xmrg_file_count = 1
      logger = None
      if 'logger' in kwargs:
        logger_name = kwargs['logger_name']
        logger_config = kwargs['logger_config']
        debug_dir = kwargs['debug_files_directory']
        # Each worker will set it's own filename for the filehandler
        base_filename = logger_config['handlers']['file_handler']['filename']
        filename_parts = os.path.split(base_filename)
        filename, ext = os.path.splitext(filename_parts[1])
        worker_filename = os.path.join(filename_parts[0], '%s_%s%s' %
                                       (filename, current_process().name.replace(':', '_'), ext))
        logger_config['handlers']['file_handler']['filename'] = worker_filename
        logging.config.dictConfig(logger_config)
        logger = logging.getLogger(logger_name)
        logger.debug("%s starting process_xmrg_file." % (current_process().name))

      inputQueue = kwargs['input_queue']
      resultsQueue = kwargs['results_queue']
      save_all_precip_vals = kwargs['save_all_precip_vals']
      # A course bounding box that restricts us to our area of interest.
      minLatLong = None
      maxLatLong = None
      if 'min_lat_lon' in kwargs and 'max_lat_lon' in kwargs:
        minLatLong = kwargs['min_lat_lon']
        maxLatLong = kwargs['max_lat_lon']

      # Boundaries we are creating the weighted averages for.
      boundaries = kwargs['boundaries']

      save_boundary_grid_cells = True
      save_boundary_grids_one_pass = True
      write_weighted_avg_debug = True

      # This is the database insert datetime.
      datetime = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

    except Exception as e:
      if logger:
        logger.exception(e)

    else:
      # Build boundary dataframes
      boundary_frames = []
      for boundary in boundaries:
        df = pd.DataFrame([[boundary.name, boundary.object_geometry]], columns=['Name', 'Boundaries'])
        boundary_df = gpd.GeoDataFrame(df, geometry=df.Boundaries)
        boundary_df = boundary_df.drop(columns=['Boundaries'])
        boundary_df.set_crs(epsg=4326, inplace=True)
        boundary_frames.append(boundary_df)
        # Write out a geojson file we can use to visualize the boundaries if needed.
        try:
          boundaries_outfile = os.path.join(debug_dir,
                                            "%s_boundary.json" % (boundary_df['Name'][0].replace(' ', '_')))
          boundary_df.to_file(boundaries_outfile, driver="GeoJSON")
        except Exception as e:
          logger.exception(e)
      for xmrg_filename in iter(inputQueue.get, 'STOP'):
        tot_file_time_start = time.time()
        if logger:
          logger.debug("ID: %s processing file: %s" % (current_process().name, xmrg_filename))

        xmrg_proc_obj = wqXMRGProcessing(logger=False)
        gpXmrg = geoXmrg(minLatLong, maxLatLong, 0.01)
        gpXmrg.openFile(xmrg_filename)

        # This is the database insert datetime.
        # Parse the filename to get the data time.
        (directory, filetime) = os.path.split(gpXmrg.fileName)
        xmrg_filename = filetime
        (filetime, ext) = os.path.splitext(filetime)
        filetime = xmrg_proc_obj.getCollectionDateFromFilename(filetime)

        if gpXmrg.readFileHeader():
          read_rows_start = time.time()
          gpXmrg.readAllRows()
          if logger:
            logger.debug("ID: %s(%f secs) to read all rows in file: %s" % (
              current_process().name, time.time() - read_rows_start, xmrg_filename))
          # Save grids to file

          gp_results = xmrg_results()
          gp_results.datetime = filetime
          # overlayed = gpd.overlay(gpXmrg._geo_data_frame, boundary_df, how="intersection")


          for index, boundary_row in enumerate(boundary_frames):
            file_start_time = time.time()
            overlayed = gpd.overlay(boundary_row, gpXmrg._geo_data_frame, how="intersection", keep_geom_type=False)

            if save_boundary_grid_cells:
              for ndx, row in overlayed.iterrows():
                gp_results.add_grid(row.Name, (row.geometry,row.Precipitation))
            # Here we create our percentage column by applying the function in the map(). This applies to
            # each area.
            overlayed['percent'] = overlayed.area.map(lambda area: float(area) / float(boundary_row.area))
            overlayed['weighted average'] = (overlayed['Precipitation']) * (overlayed['percent'])

            wghtd_avg_val = sum(overlayed['weighted average'])
            gp_results.add_boundary_result(boundary_row['Name'][0], 'weighted_average', wghtd_avg_val)
            logger.debug("ID: %s Processed file: %s in %f seconds." % \
                         (current_process().name, xmrg_filename, time.time()-file_start_time))


            if write_weighted_avg_debug and wghtd_avg_val != 0:
              wgtd_avg_file = os.path.join(debug_dir, "%s_%s_gp.csv" % (filetime.replace(':', '_'), boundary_row['Name'][0].replace(' ', '_')))
              try:
                weighted_file_obj = open(wgtd_avg_file, "w")
                weighted_file_obj.write("Percent,Precipitation,Weighted Average,Grid\n")
                for ndx, row in overlayed.iterrows():
                  weighted_file_obj.write("%s,%s,%s,%s\n"\
                                          % (row['percent'], row['Precipitation'], row['weighted average'], str(row['geometry'])))
                weighted_file_obj.close()
              except Exception as e:
                logger.exception(e)
            if wghtd_avg_val != 0:
              try:
                overlayed_results = os.path.join(debug_dir,
                                                 "%s_%s_weighted-avg_results.json" % (filetime.replace(':', '_'),
                                                                                      boundary_row.Name[0].replace(' ',
                                                                                                                   '_')))
                overlayed.to_file(overlayed_results, driver="GeoJSON")
              except Exception as e:
                raise e
            if save_boundary_grids_one_pass:
              try:
                full_data_grid = os.path.join(debug_dir,
                                              "%s_%s_fullgrid_.json" % (filetime.replace(':', '_'),
                                                                        boundary_row.Name[0].replace(' ', '_')))
                gpXmrg._geo_data_frame.to_file(full_data_grid, driver="GeoJSON")
                save_boundary_grids_one_pass = False
              except Exception as e:
                logger.exception(e)

          resultsQueue.put(gp_results)

        else:
          if logger:
            logger.error("ID: %s Process: %s Failed to process file: %s"\
                         % (current_process().name, current_process().name, xmrg_filename))

      if logger:
        logger.debug("ID: %s process finished. Processed: %d files in time: %f seconds" \
                     % (current_process().name, xmrg_file_count, time.time() - processing_start_time))
  except Exception as e:
    logger.exception(e)
  return


class wqXMRGProcessingGP(wqXMRGProcessing):
  def import_files(self, file_list):
    if self.logger:
      self.logger.debug("Start import_files" )

      workers = self.worker_process_count
      inputQueue = Queue()
      resultQueue = Queue()
      processes = []

      if self.logger:
        self.logger.debug("Importing: %d files." % (len(file_list)))
      for file_name in file_list:
        inputQueue.put(file_name)

      #Start up the worker processes.
      for workerNum in range(workers):
        args = {
          'logger': True,
          'logger_name': self.logger_name,
          'logger_config': self.logger_config,
          'input_queue': inputQueue,
          'results_queue': resultQueue,
          'min_lat_lon': self.minLL,
          'max_lat_lon': self.maxLL,
          'nexrad_schema_files': self.nexrad_schema_files,
          'nexrad_schema_directory':self.nexrad_schema_directory,
          'save_all_precip_vals': self.saveAllPrecipVals,
          'boundaries': self.boundaries,
          'spatialite_lib': self.spatiaLiteLib,
          'delete_source_file': self.deleteSourceFile,
          'delete_compressed_source_file': self.deleteCompressedSourceFile,
          'debug_files_directory': self.KMLDir
        }
        p = Process(target=process_xmrg_file_geopandas, kwargs=args)
        if self.logger:
          self.logger.debug("Starting process: %s" % (p._name))
        p.start()
        processes.append(p)
        inputQueue.put('STOP')


      #If we don't empty the resultQueue periodically, the .join() below would block continously.
      #See docs: http://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
      #the blurb on Joining processes that use queues
      '''
      self.logger.debug("Begin checking Queue for results")
      process_queues = True
      while process_queues:
        for checkJob in processes:
          if (rec_count % 10) == 0:
            self.logger.debug("Processed %d results" % (rec_count))
          if checkJob is not None and checkJob.is_alive():
            if not resultQueue.empty():
              self.process_result(resultQueue.get())
              rec_count += 1
      '''

      rec_count = 0
      self.logger.debug("Waiting for %d processes to complete" % (workers))
      while any([(checkJob is not None and checkJob.is_alive()) for checkJob in processes]):
        if not resultQueue.empty():
          #finalResults.append(resultQueue.get())
          if (rec_count % 10) == 0:
            self.logger.debug("Processed %d results" % (rec_count))
          self.process_result(resultQueue.get())
          rec_count += 1

      '''
      #Wait for the process to finish.
      self.logger.debug("Waiting for %d processes to complete" % (workers))
      for p in processes:
        self.logger.debug("Waiting for process: %s to complete" % (p._name))
        if p.is_alive():
          p.join()
        else:
          self.logger.debug("Process: %s already completed" % (p._name))
      '''
      #Poll the queue once more to get any remaining records.
      while not resultQueue.empty():
        self.logger.debug("Pulling records from resultsQueue.")
        self.process_result(resultQueue.get())
        rec_count += 1

      if self.logger:
        self.logger.debug("Imported: %d records" % (rec_count))

    if self.kmlTimeSeries:
      self.write_kml_time_series()

    if self.logger:
      self.logger.debug("Finished import_files" )

    return



def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-i", "--ImportData", dest="import_data",
                    help="Directory to import XMRG files from" )
  parser.add_option("-b", "--BackfillNHours", dest="backfill_n_hours", type="int",
                    help="Number of hours of NEXRAD data to download and process." )
  parser.add_option("-g", "--FillGaps", dest="fill_gaps",  action="store_true", default=False,
                    help="If set, this will find gaps for the past N hours as defined in the BackfillNHours." )
  parser.add_option("-s", "--StartDate", dest="start_date", default=None,
                    help="Options starting date to use for backfill or gap find operations." )

  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    configFile = ConfigParser.RawConfigParser()
    configFile.read(options.config_file)

    logger = None
    logConfFile = configFile.get('logging', 'xmrg_ingest')
    logger_name = configFile.get('logging', 'xmrg_ingest_logger_name')
    worker_logfile_name = configFile.get('logging', 'worker_logfile_name')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger(logger_name)
      logger.info("Log file opened.")

    logging_config = {
      'version': 1,
      'disable_existing_loggers': False,
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
          'level': logging.DEBUG
        },
        'file_handler': {
          'class': 'logging.handlers.RotatingFileHandler',
          'filename': worker_logfile_name,
          'formatter': 'f',
          'level': logging.DEBUG
        }
      },
      'root': {
        'handlers': ['file_handler', 'stream'],
        'level': logging.NOTSET,
        'propagate': False
      }
    }

  except ConfigParser.Error as e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  except Exception as e:
    import traceback
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    xmrg_proc = wqXMRGProcessingGP(logger=True, logger_name='nexrad_processing', logger_config=logging_config)
    xmrg_proc.load_config_settings(config_file = options.config_file)
    if options.import_data is not None:
      if logger:
        logger.info("Importing directory: %s" % (options.import_data))

      import_dirs = options.import_data.split(",")

      for import_dir in import_dirs:
        file_list = os.listdir(import_dir)
        file_list.sort()
        #If we have provided a starting date and time, remove the files that are older than
        #the provided date.

        if options.start_date is not None:
          starting_file_list = []
          starting_datetime = datetime.strptime(options.start_date, '%Y-%m-%dT%H:%M:%S')
          for file_ndx in range(len(file_list)):
            if file_list[file_ndx].find('xmrg') != -1:
              file_date = datetime.strptime(getCollectionDateFromFilename(file_list[file_ndx]), '%Y-%m-%dT%H:%M:%S')
              if file_date >= starting_datetime:
                starting_file_list = file_list[file_ndx:-1]
                break
          file_list = starting_file_list[:]
        full_path_file_list = [os.path.join(import_dir, file_name) for file_name in file_list]
        xmrg_proc.import_files(full_path_file_list)

    elif options.fill_gaps:
      if options.start_date is None:
        start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
      else:
        start_date_time = timezone('UTC').localize(datetime.strptime(options.start_date, "%Y-%m-%dT%H:%M:%S"))
      if logger:
        logger.info("Fill gaps Start time: %s Prev Hours: %d" % (start_date_time, options.backfill_n_hours))

      xmrg_proc.fill_gaps(start_date_time, options.backfill_n_hours)

    elif options.backfill_n_hours:
      start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
      if logger:
        logger.info("Backfill N Hours Start time: %s Prev Hours: %d" % (start_date_time, options.backfill_n_hours))
      file_list = xmrg_proc.download_range(start_date_time, options.backfill_n_hours)
      xmrg_proc.import_files(file_list)

  if logger:
    logger.info("Log file closed.")

if __name__ == "__main__":
  main()
