import csv
import logging.config
from wqHistoricalData import station_geometry,sampling_sites,geometry_list
from shapely.wkt import loads as wkt_loads
from shapely.geometry import mapping

class wq_site(station_geometry):
  def __init__(self, **kwargs):
    station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
    self.epa_id = kwargs['epa_id']
    self.description = kwargs['description']
    self.county = kwargs['county']
    self.extents_geometry = None
    if 'extentswkt' in kwargs:
      if kwargs['extentswkt'] is not None:
        if "MULTILINESTRING" in kwargs['extentswkt']:
          self.extents_geometry = wkt_loads(kwargs['extentswkt'])
    return

  def get_extents_coords(self):
    coords = None
    if self.extents_geometry is not None:
      mapped = mapping(self.extents_geometry)
      coords = mapped['coordinates']
    return coords

"""
florida_sample_sites
Overrides the default sampling_sites object so we can load the sites from the florida data.
"""
class wq_sample_sites(sampling_sites):
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)

  """
  Function: load_sites
  Purpose: Given the file_name in the kwargs, this will read the file and load up the sampling
    sites we are working with.
  Parameters:
    **kwargs - Must have file_name which is full path to the sampling sites csv file.
  Return:
    True if successfully loaded, otherwise False.
  """
  def load_sites(self, **kwargs):
    if 'file_name' in kwargs:
      if 'boundary_file' in kwargs:
        boundaries = geometry_list(use_logger=True)
        boundaries.load(kwargs['boundary_file'])

      try:
        header_row = ["WKT","EPAbeachID","SPLocation","Description","County","Boundary","ExtentsWKT"]
        if self.logger:
          self.logger.debug("Reading sample sites file: %s" % (kwargs['file_name']))

        sites_file = open(kwargs['file_name'], "rU")
        dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
      except IOError, e:
        if self.logger:
          self.logger.exception(e)
      else:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            add_site = False
            #The site could be in multiple boundaries, so let's search to see if it is.
            station = self.get_site(row['SPLocation'])
            if station is None:
              add_site = True
              """
              station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
              self.epa_id = kwargs['epa_id']
              self.description = kwargs['description']
              self.county = kwargs['county']

              """
              extents_wkt = None
              if 'ExtentsWKT' in row:
                extents_wkt = row['ExtentsWKT']
              station = wq_site(name=row['SPLocation'],
                                        wkt=row['WKT'],
                                        epa_id=row['EPAbeachID'],
                                        description=row['Description'],
                                        county=row['County'],
                                        extentswkt=extents_wkt)
              if self.logger:
                self.logger.debug("Processing sample site: %s" % (row['SPLocation']))
              self.append(station)
              try:
                boundaries = row['Boundary'].split(',')
                for boundary in boundaries:
                  if self.logger:
                    self.logger.debug("Sample site: %s Boundary: %s" % (row['SPLocation'], boundary))
                  boundary_geometry = boundaries.get_geometry_item(boundary)
                  if add_site:
                    #Add the containing boundary
                    station.contained_by.append(boundary_geometry)
              except AttributeError as e:
                self.logger.exception(e)
          line_num += 1
        return True
    return False
