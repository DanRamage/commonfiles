import os
import logging.config
import time
import json
from wq_sites import wq_sample_sites
def contains(list, filter):
  for x in list:
    if filter(x):
      return True
  return False

class wq_sample_data:
  def __init__(self, **kwargs):
    self._station = kwargs.get('station', None)
    self._date_time = kwargs.get('date_time', None)
    self._value = kwargs.get('value', None)

  @property
  def station(self):
    return self._station
  @station.setter
  def station(self, station):
    self._station = station

  @property
  def date_time(self):
    return self._date_time
  @date_time.setter
  def date_time(self, date_time):
    self._date_time = date_time

  @property
  def value(self):
    return self._value
  @date_time.setter
  def value(self, value):
    self._value = value

class wq_samples_collection:
  def __init__(self):
    self._wq_samples = {}

  def append(self, wq_sample):
    if type(wq_sample) is list:
      for sample in wq_sample:
        if sample.station not in self._wq_samples:
          self._wq_samples[wq_sample.station] = []
        self._wq_samples[wq_sample.station].append(sample)
    else:
      if wq_sample.station not in self._wq_samples:
        self._wq_samples[wq_sample.station] = []
      self._wq_samples[wq_sample.station].append(wq_sample)

  def __getitem__(self, name):
      return self._wq_samples[name]

  def __iter__(self):
      return iter(self._wq_samples)

  def keys(self):
      return self._wq_samples.keys()

  def items(self):
      return self._wq_samples.items()

class wq_advisories_file:
  def __init__(self, sample_sites):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.sample_sites = sample_sites

  def create_file(self, out_file_name, wq_samples):
    try:
      with open(out_file_name, "w") as out_file_obj:
        features = self.build_site_features(wq_samples)
        json_data = {
          'type': 'FeatureCollection',
          'features': features
        }
        out_file_obj.write(json.dumps(json_data, sort_keys=True))
    except (IOError, Exception) as e:
      self.logger.exception(e)

    return

  def build_feature(self, site, sample_date, values):
    beachadvisories = {
      'date': '',
      'station': site.name,
      'value': ''
    }
    if len(values):
      beachadvisories = {
        'date': sample_date,
        'station': site.name,
        'value': values
      }
    feature = {
      'type': 'Feature',
      'geometry': {
        'type': 'Point',
        'coordinates': [site.object_geometry.x, site.object_geometry.y]
      },
      'properties': {
        'locale': site.description,
        'sign': False,
        'station': site.name,
        'epaid': site.epa_id,
        'beach': site.county,
        'desc': site.description,
        'len': '',
        'test': {
          'beachadvisories': beachadvisories
        }
      }
    }
    return feature

  def build_site_features(self, wq_samples):
    start_time = time.time()
    self.logger.debug("Starting build_feature_logger")
    #Sort the data based on the date time of the sample(s).
    for site in wq_samples:
      wq_samples[site].sort(key=lambda x: x.date_time, reverse=False)

    features = []
    for site in self.sample_sites:
      bacteria_data = {}
      if site.name in wq_samples:
        site_data = wq_samples[site.name]
        bacteria_data = site_data[-1]
        feature = self.build_feature(site, bacteria_data.date_time.strftime('%Y-%m-%d %H:%M:%S'), [bacteria_data.value])
      else:
        feature = self.build_feature(site, "", [])

      self.logger.debug("Adding feature site: %s Desc: %s" % (site.name, site.description))
      features.append(feature)
    self.logger.debug("Finished build_feature_logger in %f seconds" % (time.time()-start_time))
    return features


class wq_station_advisories_file:
  def __init__(self, sample_site):
    self.logger = logging.getLogger(self.__class__.__name__)
    self.sample_site = sample_site

  def create_file(self, out_file_directory, wq_samples):
    start_time = time.time()
    self.logger.debug("Starting create_file")
    station_filename = os.path.join(out_file_directory, "%s.json" % (self.sample_site.name))
    beach_advisories = []
    if self.sample_site.name in wq_samples:
      samples = wq_samples[self.sample_site.name]
      for sample in samples:
        beach_advisories.append({
          'date': sample.date_time.strftime('%Y-%m-%d %H:%M:%S'),
          'station': self.sample_site.name,
          'value': [sample.value]
        })
    if os.path.isfile(station_filename):
      try:
        self.logger.debug("Opening station JSON file: %s" % (station_filename))
        with open(station_filename, 'r') as station_json_file:
          feature = json.loads(station_json_file.read())
          if feature is not None:
            if 'test' in feature['properties']:
              file_beachadvisories = feature['properties']['test']['beachadvisories']
            else:
              file_beachadvisories = []
            # Make sure the date is not already in the list.
            for test_data in beach_advisories:
              if not contains(file_beachadvisories, lambda x: x['date'] == test_data['date']):
                self.logger.debug("Station: %s adding date: %s" % (self.sample_site.name, test_data['date']))
                file_beachadvisories.append(test_data)
                file_beachadvisories.sort(key=lambda x: x['date'], reverse=False)
      except (json.JSONDecodeError, IOError, Exception) as e:
        if self.logger:
          self.logger.exception(e)
    else:
      self.logger.debug("Creating new station JSON file for: %s" % (self.sample_site.name))

      feature = {
        'type': 'Feature',
        'geometry': {
          'type': 'Point',
          'coordinates': [self.sample_site.object_geometry.x, self.sample_site.object_geometry.y]
        },
        'properties': {
          'locale': self.sample_site.description,
          'sign': False,
          'station': self.sample_site.name,
          'epaid': self.sample_site.epa_id,
          'beach': self.sample_site.county,
          'desc': self.sample_site.description,
          'len': '',
          'test': {
            'beachadvisories': beach_advisories
          }
        }
      }
    try:
      if feature is not None:
        self.logger.debug("Creating file: %s" % (station_filename))
        with open(station_filename, 'w') as station_json_file:
          feature_json = json.dumps(feature)
          #self.logger.debug("Feature: %s" % (feature_json))
          station_json_file.write(feature_json)
      else:
        self.logger.error("Feature is None")
    except (json.JSONDecodeError, IOError) as e:
      self.logger.exception(e)

    self.logger.debug("Finished create_file in %f seconds" % (time.time() - start_time))
