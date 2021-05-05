import logging.config
import csv
import requests
from collections import OrderedDict
import codecs

class sample_stations_file(OrderedDict):
  #0 = met, 1 = wq, 2 = nut, no telemetry.

  MET_STATION = 0
  WQ_STATION = 1
  NUT_STATION = 2

  header_row = [
  "Row",
  "NERR Site ID ",
  "Station Code",
  "Station Name",
  "Lat Long",
  "Latitude ",
  "Longitude",
  "Status",
  "Active Dates",
  "State",
  "Reserve Name",
  "Real Time",
  "HADS ID",
  "GMT Offset",
  "Station Type",
  "Region",
  "isSWMP"
  ]
  def __init__(self, use_logging):
    OrderedDict.__init__(self)
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

  def download_file(self, remote_filename_url, destination_file):
    if self.logger:
      self.logger.debug("Downloading file: %s" % (remote_filename_url))
    try:
      r = requests.get(remote_filename_url, stream=True)
    except (requests.HTTPError, requests.ConnectionError) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      if r.status_code == 200:
        if self.logger:
          self.logger.debug("Saving to file: %s" % (destination_file))

        try:
          with open(destination_file, 'w') as sample_stations_file:
            sample_stations_file.write(r.content)
            """
            for chunk in r:
              sample_stations_file.write(chunk)
            """
        except IOError as e:
          if self.logger:
            self.logger.exception(e)

  def open(self, file_name):
    try:
      reserve_file = open(file_name, "r")
      dict_file = csv.DictReader(reserve_file, delimiter=',', quotechar='"', fieldnames=sample_stations_file.header_row)
    except IOError as e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        line_num = 0
        station_count = 0
        for row in dict_file:
          if line_num > 0:
            station_code = row["Station Code"].strip()
            if station_code not in self:
              if self.logger:
                self.logger.debug("Adding station: %s" % (station_code))

              self[station_code] = {
                 'state': row["State"].strip().upper(),
                 'reserve_name': row["Reserve Name"].strip(),
                 'station_code': row["Station Code"].strip(),
                 'station_name': row["Station Name"].strip(),
                 'station_type': int(row["Station Type"].strip()),
                 'real_time': row["Real Time"].strip(),
                 'reserve_code': row["NERR Site ID "].strip().upper(),
                 'lat_lon': row['Lat Long'].strip(),
                 'longitude': row["Longitude"].strip(),
                 'latitude': row["Latitude "].strip(),
                 'status': row['Status'].strip(),
                 'state': row['State'].strip(),
                 'gmt_offset': row['GMT Offset'].strip(),
                 'active_dates': row["Active Dates"].strip(),
                 'hads_id': row["HADS ID"].strip(),
                 'region': row["Region"].strip(),
                 'is_swmp': row["isSWMP"].strip()
              }
              station_count += 1
          line_num += 1

        if self.logger:
          self.logger.debug("%d station info processed" % (station_count))

        reserve_file.close()

        ret_val = True

      except Exception as e:
        if self.logger:
          self.logger.exception(e)

    if self.logger:
      self.logger.debug("Finish reading reserve info file: %s" % (file_name))

    return False