"""
Revisions
Author: DWR
Date: 2012-06-21
Function: archiveXMRGFiles
Changes: Added function to archive the XMRG files to keep the main download directory clean.

Date: 2011-07-27
Function: vacuum
Changes: Use the dhecXMRGProcessing object to vacuum the database instead of the processDHECRainGauges object.
  processDHECRainGauges is being deprecated.
"""
import sys
import optparse
import time
import datetime
import logging.config
from xmrgFile import xmrgCleanup

def archiveXMRGFiles(src_dir, target_dir):
  cleanUp = xmrgCleanup(src_dir, target_dir)
  cleanUp.organizeFilesIntoDirectories(datetime.datetime.utcnow())
  
if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-a", "--ArchiveXMRGFiles", dest="archiveXMRG", action= 'store_true',
                    help="If true, then files in the XMRG download directory are moved to the archival directory." )
  (options, args) = parser.parse_args()
  if( options.xmlConfigFile == None ):
    parser.print_usage()
    parser.print_help()
    sys.exit(-1)

  configSettings = xmlConfigFile(options.xmlConfigFile)

  logFile = configSettings.getEntry('//environment/logging/logConfigFile')
  logging.config.fileConfig(logFile)
  logger = logging.getLogger("dhec_processing_logger")
  logger.info("Session started")
  
  if(options.archiveXMRG):
    archiveXMRGFiles(options.xmlConfigFile)

  logger.info("Session stopped")