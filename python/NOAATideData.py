import sys
sys.path.append('../commonfiles/python')

from suds.client import Client
from suds import WebFault
import time
from datetime import datetime, timedelta
from pytz import timezone as pytz_timezone
from date_time_utils import get_utc_epoch, datetime2matlabdn
#import operator
import array
from lxml import objectify,etree
from lxml.etree import XMLParser
#from numpy import NaN, Inf, arange, isscalar, asarray, sqrt, mean, square
import numpy as np

from peakdetect_algorithms import peakdetect as pda_peakdetect
#import matplotlib
#matplotlib.use('Agg')
#import matplotlib.pyplot as plt

#import numpy as np

class noaaTideData(object):
  LOW_TIDE = 0
  HI_TIDE = 1
  
  def __init__(self, use_raw=True, logger=None):
    self.tideChangesRawData = []
    self.tideChangesSmoothData = []
    self.use_raw = use_raw
    self.baseUrl = 'http://opendap.co-ops.nos.noaa.gov/axis/webservices/waterlevelrawsixmin/wsdl/WaterLevelRawSixMin.wsdl'
    if not self.use_raw:
      self.baseUrl = "http://opendap.co-ops.nos.noaa.gov/axis/webservices/waterlevelverifiedsixmin/wsdl/WaterLevelVerifiedSixMin.wsdl"
    self.logger = logger     

  """
  beginDate is the date we wish to start our data query at. Format is: YYYYMMDD
  endDate is the date we wish to end our data query at. Format is: YYYYMMDD
  waterLevelSensorDataType is the type of water level we want. Can be: 
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels. 
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST  
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """
  def getWaterLevelRawSixMinuteData(self,
                                    beginDate, 
                                    endDate, 
                                    station,
                                    datum='MLLW', 
                                    unit='feet',                                    
                                    shift='GMT',
                                    retxml = False):
    soapClient = Client(self.baseUrl, retxml=retxml)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    data = soapClient.service.getWaterLevelRawSixMin(station, beginDate, endDate, datum, unit, shift)

    return(data)

  """
  beginDate is a datetime object we wish to start our data query at.
  endDate is a datetime object we wish to end our data query at.
  waterLevelSensorDataType is the type of water level we want. Can be:
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels.
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """
  def getWaterLevelVerifiedSixMinuteData(self,
                                    beginDate,
                                    endDate,
                                    station,
                                    datum='MLLW',
                                    unit='feet',
                                    shift='GMT',
                                    retxml = False ):
    soapClient = Client(self.baseUrl, timeout=90, retxml=retxml)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    data = soapClient.service.getWaterLevelVerifiedSixMin(station, beginDate, endDate, datum, unit, shift)

    return(data)

  def calcTideRange(self,           
                    beginDate, 
                    endDate, 
                    station,
                    datum='MLLW', 
                    units='feet',
                    timezone='GMT',
                    smoothData=False,                    
                    tideFileDir=None):

    #This is the dictionary we return. Its keys are the tide indicators: LL is Lowest Low Tide, L is Low Tide, HH Highest High Tide, H High tide.
    tideData = {}
    tideData['LL'] = None
    tideData['HH'] = None
    tideData['L'] = None
    tideData['H'] = None
    tideData['PeakValue'] = None
    tideData['ValleyValue'] = None
    tideData['tide_stage'] = None
    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteData(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteData(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
    except (WebFault,Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      chordLen = 10

      #Determine the tide level using all the tide data points.
      #tideData['tide_stage'] = self.get_tide_stage(wlData, chordLen, endDate, timezone)

      smoothDataROC = array.array('d')
      rawDataROC = array.array('d')
      expSmoothedData =  array.array('d')
      dataLen = len(wlData.item)
      ndx = 0
      alpha = 0.5
      utc_tz = pytz_timezone('UTC')
      start_ndx = None
      end_ndx = None
      for ndx in range(0, dataLen):
        wl_time = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= beginDate:
          start_ndx = ndx
        if end_ndx is None and wl_time > endDate:
          end_ndx = ndx-1
      wlData.item = wlData.item[start_ndx:end_ndx]
      dataLen = len(wlData.item)
      for ndx in range(0, dataLen):
        valN = wlData.item[ndx]['WL']
        #tidePts.append(valN)
        #data_ts = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        #timePts.append(int(get_utc_epoch(data_ts)))
        #Then the formula for each successive point is (alpha * Xn) + (1-alpha) * Yn-1
        #X is the original data, Yn-1 is the last smoothed data point, alpha is the smoothing constant.
        if ndx == 0:
          expSmoothedData.append(valN)
          tideMin1 = valN
          tideMax1 = valN
          tideMin2 = valN
          tideMax2 = valN

        else:
          timeStruct = utc_tz.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          timeN = int(get_utc_epoch(timeStruct))
          timeStruct = utc_tz.localize(datetime.strptime(wlData.item[ndx-1]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
          timeN1 = int(get_utc_epoch(timeStruct))

          #For each N+1 we now use the formula.
          Yn = (alpha * wlData.item[ndx]['WL']) + ((1 - alpha) * expSmoothedData[ndx-1])
          expSmoothedData.append(Yn)

          smoothDataROC.append((expSmoothedData[ndx] - expSmoothedData[ndx-1]) / (timeN - timeN1))

          #Calcuate the rateofchange
          #ROC for the raw data.
          valN1 = wlData.item[ndx-1]['WL']
          rawDataROC.append((valN - valN1) / (timeN - timeN1))



      #ndx = 0
      a = None
      b = None
      c = None
      #dirChangeCnt = 0
      midPt = chordLen / 2
      #ptFound = False
      #stopProc = False
      #dataLen = len(wlData.item)
      #slopePositive = False

      #plt.plot(timePts,tidePts,'o', x_new, y_new)
      ##plt.xlim([timePts[0]-1, timePts[-1] + 1 ])
      #plt.savefig('/users/danramage/tmp/out.png', dpi=96)

      if self.logger:
        self.logger.info("Checking Raw data.")

      self.find_tide_change_points(wlData.item, chordLen, tideData)
      """
      tideChange = None
      changeNdx = None
      lastSlope = None
      for ndx in range(0, len(wlData.item)):
        a = wlData.item[ndx]['WL']
        timeStamp = wlData.item[ndx].timeStamp
        if ndx + chordLen < dataLen - 1:
          c = wlData.item[ndx+chordLen]['WL']
        else:
          stopProc = True
        if tideChange is None:
          tideChange = a
          tide_change_ts = timeStamp
        if not stopProc:
          #Calc slope
          #Ascending
          if c - a > 0:
            if lastSlope == 0:
              if tideData['LL'] is None:
                #tideData['LL'] = tideChange
                tideData['LL'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
              elif tideChange < tideData['LL']['value']:
                tmp = tideData['LL']
                #tideData['LL'] = tideChange
                tideData['LL'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
                tideData['L'] = tmp
              else:
                tideData['L'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              #print("Tide Min at: %f@%s" %(tideChange,timeStamp))
              if self.logger:
                self.logger.debug("Tide Min at: %f@%s" %(tideChange,tide_change_ts))
              #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
              ndx += chordLen
              #Slope has changed direction.
              lastSlope = 1
              continue
            lastSlope = 1

            if(a > tideChange):
              tideChange = a
              tide_change_ts = timeStamp
              changeNdx = ndx
          #Descending
          elif c - a < 0:
            if lastSlope == 1:
              if tideData['HH'] is None:
                #tideData['HH'] = tideChange
                tideData['HH'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              elif tideChange > tideData['HH']['value']:
                tmp = tideData['HH']
                #tideData['HH'] = tideChange
                tideData['HH'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }
                tideData['H'] = tmp
              else:
                tideData['H'] = {
                                  'value' : tideChange,
                                  'date' : tide_change_ts
                                 }

              #print("Tide Max at: %f@%s" %(tideChange,timeStamp))
              if self.logger:
                self.logger.debug("Tide Max at: %f@%s" %(tideChange,tide_change_ts))

              #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
              ndx += chordLen
              #Slope has changed direction.
              lastSlope = 0
              continue
            lastSlope = 0

            if a < tideChange:
              tideChange = a
              tide_change_ts = timeStamp

              changeNdx = ndx
        #Save off the highest and lowest values.
        if tideData['PeakValue'] is None or tideData['PeakValue']['value'] < a:
          tideData['PeakValue'] = {'value': a,
                                    'date': timeStamp}
        if tideData['ValleyValue'] is None or tideData['ValleyValue']['value'] > a:
          tideData['ValleyValue'] = {'value': a,
                                    'date': timeStamp}

        ndx += 1
      """
      if smoothData:
        print("Checking smoothed data.")
        dataLen = len(expSmoothedData)
        ndx = 0
        ptFound = False
        stopProc = False

        while ndx < dataLen:
          a = expSmoothedData[ndx]
          if ndx + midPt < dataLen - 1:
            b = expSmoothedData[ndx+midPt]
          else:
            stopProc = True
          if ndx + chordLen < dataLen - 1:
            c = expSmoothedData[ndx+chordLen]
          else:
            stopProc = True
          if stopProc == False:
            #Calc slope
            if c - a > 0:
              if b > a and b > c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger != None:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))

                ptFound = True
            elif c - a < 0:
              if b < a and b < c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                ptFound = True
          if ptFound == False:
            ndx += 1
          else:
            ndx = ndx+midPt
            ptFound = False

      if tideFileDir != None:
        filename = "%s\\%s-%s.csv" %(tideFileDir,beginDate,endDate)
        tideFile = open(filename, "w")

        ndx = 0
        dataLen = len(wlData.item)
        while ndx < dataLen:
          timeStruct = time.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0')
          seconds = time.mktime(timeStruct)
          medianROC = ''
          rawROC = ''
          smoothedData = ''
          if(ndx < len(rawDataROC)):
            rawROC = rawDataROC[ndx]
            smoothedROC = smoothDataROC[ndx]
            smoothedData = expSmoothedData[ndx]

          outbuf = "%s,%s,%s,%s,%s\n" %(seconds,wlData.item[ndx]['WL'], rawROC, smoothedData, smoothedROC)
          ndx += 1
          tideFile.write(outbuf)
        tideFile.close()
    #If we didn't have all the inflection points, we'll use the peak/valley values for the missing one(s).


    return(tideData)

  def find_tide_change_points(self, tide_recs, chordLen, tideData):
    a = None
    b = None
    c = None
    stopProc = False

    tideChange = None
    changeNdx = None
    lastSlope = None
    dataLen = len(tide_recs)
    for ndx in range(0, len(tide_recs)):
      a =tide_recs[ndx]['WL']
      timeStamp = tide_recs[ndx].timeStamp
      if ndx + chordLen < dataLen - 1:
        c = tide_recs[ndx+chordLen]['WL']
      else:
        stopProc = True
      if tideChange is None:
        tideChange = a
        tide_change_ts = timeStamp
      if not stopProc:
        #Calc slope
        #Ascending
        if c - a > 0:
          if lastSlope == 0:
            if tideData['LL'] is None:
              #tideData['LL'] = tideChange
              tideData['LL'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }
            elif tideChange < tideData['LL']['value']:
              tmp = tideData['LL']
              #tideData['LL'] = tideChange
              tideData['LL'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }
              tideData['L'] = tmp
            else:
              tideData['L'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }

            #print("Tide Min at: %f@%s" %(tideChange,timeStamp))
            if self.logger:
              self.logger.debug("Tide Min at: %f@%s" %(tideChange,tide_change_ts))
            #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
            ndx += chordLen
            #Slope has changed direction.
            lastSlope = 1
            continue
          lastSlope = 1

          if(a > tideChange):
            tideChange = a
            tide_change_ts = timeStamp
            changeNdx = ndx
        #Descending
        elif c - a < 0:
          if lastSlope == 1:
            if tideData['HH'] is None:
              #tideData['HH'] = tideChange
              tideData['HH'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }

            elif tideChange > tideData['HH']['value']:
              tmp = tideData['HH']
              #tideData['HH'] = tideChange
              tideData['HH'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }
              tideData['H'] = tmp
            else:
              tideData['H'] = {
                                'value' : tideChange,
                                'date' : tide_change_ts
                               }

            #print("Tide Max at: %f@%s" %(tideChange,timeStamp))
            if self.logger:
              self.logger.debug("Tide Max at: %f@%s" %(tideChange,tide_change_ts))

            #Found the max tide, so another is not going to occur any quicker than the chord length, so increment the ndx.
            ndx += chordLen
            #Slope has changed direction.
            lastSlope = 0
            continue
          lastSlope = 0

          if a < tideChange:
            tideChange = a
            tide_change_ts = timeStamp

            changeNdx = ndx
      #Save off the highest and lowest values.
      if tideData['PeakValue'] is None or tideData['PeakValue']['value'] < a:
        tideData['PeakValue'] = {'value': a,
                                  'date': timeStamp}
      if tideData['ValleyValue'] is None or tideData['ValleyValue']['value'] > a:
        tideData['ValleyValue'] = {'value': a,
                                  'date': timeStamp}

      ndx += 1

  def get_tide_stage(self, begin_date,
                            end_date,
                            station,
                            datum='MLLW',
                            units='feet',
                            time_zone='GMT',
                            write_tide_data=False):
    tide_data = { 'LL': None,
    'HH': None,
    'L': None,
    'H': None,
    'PeakValue': None,
    'ValleyValue': None }
    tide_stage = -9999
    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteData(begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'), station, datum, units, time_zone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteData(begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'), station, datum, units, time_zone)
    except (WebFault,Exception) as e:
      if self.logger:
        self.logger.exception(e)

    tz_obj = None
    if time_zone == 'GMT':
      tz_obj = pytz_timezone('UTC')

    #if begin_time == tz_obj.localize(datetime.strptime('2001-08-27 04:00:00', '%Y-%m-%d %H:%M:%S')):
    #  i = 0
    try:
      start_time_ndx = end_date - timedelta(hours=10)
      end_time_ndx = end_date + timedelta(hours=10)
      start_ndx = None
      end_ndx = None
      for ndx in range(0, len(wlData.item)):
        wl_time = tz_obj.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= start_time_ndx:
          start_ndx = ndx
        if end_ndx is None and wl_time > end_time_ndx:
          end_ndx = ndx-1

      #tide_recs = wlData.item[start_ndx:end_ndx]
      tide_recs = wlData.item[start_ndx:end_ndx]
      #self.find_tide_change_points(tide_recs, chordLen, tide_data)

      recs = [tide_recs[ndx]['WL'] for ndx, data in enumerate(tide_recs)]
      #Get RMS of data
      #maxtab, mintab = peakdet(recs, 0.08)
      pda_maxtab, pda_mintab = pda_peakdetect(recs, None, 10, 0, False)

      #zero_maxtab, zero_mintab = peakdetect_zero_crossing(y_axis=recs, x_axis=None, window=13)
      #Sort the maxs and mins by value then date.
      #max_sorted = sorted(pda_maxtab, key=lambda rec: (tide_recs[int(rec[0])]['WL'], tide_recs[int(rec[0])]['timeStamp']))
      #min_sorted = sorted(pda_mintab, key=lambda rec: (tide_recs[int(rec[0])]['WL'], tide_recs[int(rec[0])]['timeStamp']))
      max_len = len(pda_maxtab) - 1
      tide_data['HH'] = {
        'value': tide_recs[int(pda_maxtab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_maxtab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['H'] = {
          'value': tide_recs[int(pda_maxtab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_maxtab[max_len-1][0])]['timeStamp']
        }
      max_len = len(pda_mintab) - 1
      tide_data['LL'] = {
        'value': tide_recs[int(pda_mintab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_mintab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['L'] = {
          'value': tide_recs[int(pda_mintab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_mintab[max_len-1][0])]['timeStamp']
        }
      tide_levels = ['H','HH', 'L', 'LL']
      tide_changes = [tide_data[tide_level] for tide_level in tide_levels if tide_level in tide_data and tide_data[tide_level] is not None]
      tide_changes = sorted(tide_changes, key=lambda k: k['date'])

      #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
      #the 2000 for flood or 4000 for ebb.
      tide_stages = [0, 100, 200, 300]
      prev_tide_data_rec = None
      tolerance = timedelta(hours = 1)
      for tide_sample in tide_changes:
        if prev_tide_data_rec is not None:
          prev_date_time = tz_obj.localize(datetime.strptime(prev_tide_data_rec['date'], '%Y-%m-%d %H:%M:%S.0'))
          cur_date_time = tz_obj.localize(datetime.strptime(tide_sample['date'], '%Y-%m-%d %H:%M:%S.0'))
          if (end_date >= prev_date_time - tolerance or end_date >= prev_date_time + tolerance)\
            and (end_date < cur_date_time - tolerance or end_date < cur_date_time + tolerance):
            prev_level = prev_tide_data_rec['value']
            cur_level = tide_sample['value']
            if prev_level < cur_level:
              tide_state = 2000
            else:
              tide_state = 4000

            #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
            #up into 4 pieces, then figure out where our query time falls.
            time_delta = cur_date_time - prev_date_time
            qtr_time = time_delta.total_seconds() / 4.0
            prev_time = prev_date_time
            for i in range(0, 4):
              if end_date >= prev_time and end_date < (prev_time + timedelta(seconds=qtr_time)):
                 tide_stage = tide_state + tide_stages[i]
                 break

              prev_time = prev_time + timedelta(seconds=qtr_time)

        if tide_stage != -9999:
          break
        prev_tide_data_rec = tide_sample


    except Exception, e:
      if self.logger:
        self.logger.exception(e)
    if write_tide_data:
      with open('/Users/danramage/tmp/florida_data/tide_stage_data/%s.csv' % (end_date.strftime('%Y-%m-%d_%H_%M')), 'w') as tide_data_out:
        for rec in tide_recs:
          tide_data_out.write("%s,%f\n" % (rec['timeStamp'], rec['WL']))

    return tide_stage


class noaaTideDataExt(noaaTideData):
  """
  beginDate is the date we wish to start our data query at. Format is: YYYYMMDD
  endDate is the date we wish to end our data query at. Format is: YYYYMMDD
  waterLevelSensorDataType is the type of water level we want. Can be:
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels.
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """
  """
  beginDate is a datetime object we wish to start our data query at.
  endDate is a datetime object we wish to end our data query at.
  waterLevelSensorDataType is the type of water level we want. Can be:
    W1 = Six minute interval data
    W2 = Hourly water level data
    W3 = Daily high/low water level data
    W4 = Monthly high/low water level data
  relative
  datum A tidal datum is a standard elevation used as a reference to measure water levels.
    MLLW, or Mean Lower Low Water, is the default datum. Other datum options  are MHHW, MHW, MTW, MSL, MLW, and Station Datum.
  unit is the units of measurement, options are feet or meters.
  shift is the time zone the data should be in. Can be: Local, GMT, LST
  station is the station name in the form of "ID Name, State" for example: 8661070 Springmaid Pier,SC&
  type is Historic Tide Data, most likely this changes when looking for predicted ranges.
  format can be View Data or View Plot
  """

  def getWaterLevelRawSixMinuteDataExt(self,
                                    beginDate,
                                    endDate,
                                    station,
                                    datum='MLLW',
                                    unit='feet',
                                    shift='GMT'):
    soapClient = Client(self.baseUrl, retxml=True)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    ret_xml = soapClient.service.getWaterLevelRawSixMin(station, beginDate, endDate, datum, unit, shift)
    if self.logger:
      self.logger.debug(ret_xml)
    parser = XMLParser(remove_blank_text=True, huge_tree=True)
    parser.set_element_class_lookup(objectify.ObjectifyElementClassLookup())
    objectify.set_default_parser(parser)
    root = objectify.fromstring(ret_xml)
    objectify.deannotate(root, cleanup_namespaces=True)

    return(root)

  def getWaterLevelVerifiedSixMinuteDataExt(self,
                                    beginDate,
                                    endDate,
                                    station,
                                    datum='MLLW',
                                    unit='feet',
                                    shift='GMT'):
    soapClient = Client(self.baseUrl, timeout=90, retxml=True)
    if(unit == 'feet'):
      unit = 1
    else:
      unit = 2
    if(shift == 'GMT'):
      shift = 0
    else:
      shift = 1

    data = soapClient.service.getWaterLevelVerifiedSixMin(station, beginDate, endDate, datum, unit, shift)

    return(data)

  def calcTideRangeExt(self,
                    beginDate,
                    endDate,
                    station,
                    datum='MLLW',
                    units='feet',
                    timezone='GMT',
                    smoothData=False,
                    tideFileDir=None,
                    write_tide_data=False):

    #This is the dictionary we return. Its keys are the tide indicators: LL is Lowest Low Tide, L is Low Tide, HH Highest High Tide, H High tide.
    tideData = None
    pda_tide_data = None
    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteDataExt(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteDataExt(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
    except (WebFault, Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      tideData = {}
      tideData['LL'] = None
      tideData['HH'] = None
      tideData['L'] = None
      tideData['H'] = None
      tideData['PeakValue'] = None
      tideData['ValleyValue'] = None
      tideData['tide_stage'] = None

      chordLen = 10
      #Determine the tide level using all the tide data points.
      #tideData['tide_stage'] = self.get_tide_stage(wlData, chordLen, endDate, timezone)

      smoothDataROC = array.array('d')
      rawDataROC = array.array('d')
      expSmoothedData =  array.array('d')

      #dataLen = len(wlData.item)
      ndx = 0
      alpha = 0.5
      utc_tz = pytz_timezone('UTC')
      start_ndx = None
      end_ndx = None
      #for ndx in range(0, dataLen):
      #It's seemingly impossible to use object notation to navigate to the data.
      data_start_tag = wlData.Body.getchildren()[0].getchildren()[0].item
      dataLen = len(data_start_tag)
      for ndx in range(0, dataLen):
        wl_time = utc_tz.localize(datetime.strptime(data_start_tag[ndx]['timeStamp'].text, '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= beginDate:
          start_ndx = ndx
        if end_ndx is None and wl_time > endDate:
          end_ndx = ndx-1
      data_start_tag = data_start_tag[start_ndx:end_ndx]
      dataLen = len(data_start_tag)
      for ndx in range(0, dataLen):
        valN = data_start_tag[ndx]['WL']
        #tidePts.append(valN)
        #data_ts = utc_tz.localize(datetime.strptime(data_start_tag[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        #timePts.append(int(get_utc_epoch(data_ts)))
        #Then the formula for each successive point is (alpha * Xn) + (1-alpha) * Yn-1
        #X is the original data, Yn-1 is the last smoothed data point, alpha is the smoothing constant.
        if ndx == 0:
          expSmoothedData.append(valN)
          tideMin1 = valN
          tideMax1 = valN
          tideMin2 = valN
          tideMax2 = valN

        else:
          timeStruct = utc_tz.localize(datetime.strptime(data_start_tag[ndx]['timeStamp'].text, '%Y-%m-%d %H:%M:%S.0'))
          timeN = int(get_utc_epoch(timeStruct))
          timeStruct = utc_tz.localize(datetime.strptime(data_start_tag[ndx-1]['timeStamp'].text, '%Y-%m-%d %H:%M:%S.0'))
          timeN1 = int(get_utc_epoch(timeStruct))

          #For each N+1 we now use the formula.
          Yn = (alpha * data_start_tag[ndx]['WL']) + ((1 - alpha) * expSmoothedData[ndx-1])
          expSmoothedData.append(Yn)

          smoothDataROC.append((expSmoothedData[ndx] - expSmoothedData[ndx-1]) / (timeN - timeN1))

          #Calcuate the rateofchange
          #ROC for the raw data.
          valN1 = data_start_tag[ndx-1]['WL']
          rawDataROC.append((valN - valN1) / (timeN - timeN1))



      #ndx = 0
      a = None
      b = None
      c = None
      #dirChangeCnt = 0
      midPt = chordLen / 2
      #ptFound = False
      #stopProc = False
      #dataLen = len(wlData.item)
      #slopePositive = False

      #plt.plot(timePts,tidePts,'o', x_new, y_new)
      ##plt.xlim([timePts[0]-1, timePts[-1] + 1 ])
      #plt.savefig('/users/danramage/tmp/out.png', dpi=96)

      if self.logger:
        self.logger.info("Checking Raw data.")

      self.find_tide_change_points(data_start_tag, chordLen, tideData)

      if smoothData:
        print("Checking smoothed data.")
        dataLen = len(expSmoothedData)
        ndx = 0
        ptFound = False
        stopProc = False

        while ndx < dataLen:
          a = expSmoothedData[ndx]
          if ndx + midPt < dataLen - 1:
            b = expSmoothedData[ndx+midPt]
          else:
            stopProc = True
          if ndx + chordLen < dataLen - 1:
            c = expSmoothedData[ndx+chordLen]
          else:
            stopProc = True
          if stopProc == False:
            #Calc slope
            if c - a > 0:
              if b > a and b > c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger != None:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))

                ptFound = True
            elif c - a < 0:
              if b < a and b < c:
                #print("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                if self.logger:
                  self.logger.debug("Tide change at Ndx: %d Val: %f" %(ndx+midPt, b))
                ptFound = True
          if ptFound == False:
            ndx += 1
          else:
            ndx = ndx+midPt
            ptFound = False

      if tideFileDir != None:
        filename = "%s\\%s-%s.csv" %(tideFileDir,beginDate,endDate)
        tideFile = open(filename, "w")

        ndx = 0
        dataLen = len(wlData.item)
        while ndx < dataLen:
          timeStruct = time.strptime(data_start_tag[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0')
          seconds = time.mktime(timeStruct)
          medianROC = ''
          rawROC = ''
          smoothedData = ''
          if(ndx < len(rawDataROC)):
            rawROC = rawDataROC[ndx]
            smoothedROC = smoothDataROC[ndx]
            smoothedData = expSmoothedData[ndx]

          outbuf = "%s,%s,%s,%s,%s\n" %(seconds,data_start_tag[ndx]['WL'], rawROC, smoothedData, smoothedROC)
          ndx += 1
          tideFile.write(outbuf)
        tideFile.close()
      #If we didn't have all the inflection points, we'll use the peak/valley values for the missing one(s).

      recs = [data_start_tag[ndx]['WL'] for ndx, data in enumerate(data_start_tag)]
      #Get RMS of data
      #maxtab, mintab = peakdet(recs, 0.08)
      pda_maxtab, pda_mintab = pda_peakdetect(recs, None, 10, 0, False)
      pda_tide_data = {}
      pda_tide_data['LL'] = None
      pda_tide_data['HH'] = None
      pda_tide_data['L'] = None
      pda_tide_data['H'] = None
      pda_tide_data['PeakValue'] = None
      pda_tide_data['ValleyValue'] = None
      pda_tide_data['tide_stage'] = None

      try:
        if len(pda_maxtab) > 0:
          maxes = sorted(pda_maxtab, key=lambda rec: rec[1])
          max_len = len(pda_maxtab) - 1
          pda_tide_data['HH'] = {
            'value': data_start_tag[int(maxes[max_len][0])]['WL'],
            'date':  data_start_tag[int(maxes[max_len][0])]['timeStamp']
          }
          if max_len > 0:
            pda_tide_data['H'] = {
              'value': data_start_tag[int(maxes[max_len-1][0])]['WL'],
              'date':  data_start_tag[int(maxes[max_len-1][0])]['timeStamp']
            }

        if len(pda_mintab):
          mins = sorted(pda_mintab, key=lambda rec: rec[1], reverse=True)
          max_len = len(pda_mintab) - 1
          pda_tide_data['LL'] = {
            'value': data_start_tag[int(mins[max_len][0])]['WL'],
            'date':  data_start_tag[int(mins[max_len][0])]['timeStamp']
          }
          if max_len > 0:
            pda_tide_data['L'] = {
              'value': data_start_tag[int(mins[max_len-1][0])]['WL'],
              'date':  data_start_tag[int(mins[max_len-1][0])]['timeStamp']
            }
      except Exception as e:
        if self.logger:
          self.logger.exception(e)

      if write_tide_data:
        with open('/Users/danramage/tmp/%s.csv' % (endDate.strftime('%Y-%m-%d_%H_%M')), 'w') as tide_data_out:
          for rec in data_start_tag:
            tide_data_out.write("%s,%f\n" % (rec['timeStamp'], rec['WL']))

    return(tideData,pda_tide_data)

  def calcTideRangePeakDetect(self,
                    beginDate,
                    endDate,
                    station,
                    datum='MLLW',
                    units='feet',
                    timezone='GMT',
                    smoothData=False):

    #This is the dictionary we return. Its keys are the tide indicators: LL is Lowest Low Tide, L is Low Tide, HH Highest High Tide, H High tide.
    tideData = None
    pda_tide_data = None
    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteDataExt(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteDataExt(beginDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'), station, datum, units, timezone)
    except (WebFault, Exception) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      utc_tz = pytz_timezone('UTC')
      start_ndx = None
      end_ndx = None
      #for ndx in range(0, dataLen):
      #It's seemingly impossible to use object notation to navigate to the data.
      data_start_tag = wlData.Body.getchildren()[0].getchildren()[0].item
      dataLen = len(data_start_tag)
      #Get the previous 24 hours of data we are interested in.
      for ndx in range(0, dataLen):
        wl_time = utc_tz.localize(datetime.strptime(data_start_tag[ndx]['timeStamp'].text, '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= beginDate:
          start_ndx = ndx
        if end_ndx is None and wl_time > endDate:
          end_ndx = ndx-1

      data_start_tag = data_start_tag[start_ndx:end_ndx]
      recs = [data_start_tag[ndx]['WL'] for ndx, data in enumerate(data_start_tag)]
      pda_maxtab, pda_mintab = pda_peakdetect(recs, None, 10, 0, False)
      pda_tide_data = {}
      pda_tide_data['LL'] = None
      pda_tide_data['HH'] = None
      pda_tide_data['L'] = None
      pda_tide_data['H'] = None
      pda_tide_data['PeakValue'] = None
      pda_tide_data['ValleyValue'] = None
      pda_tide_data['tide_stage'] = None

      try:
        if len(pda_maxtab) > 0:
          maxes = sorted(pda_maxtab, key=lambda rec: rec[1])
          max_len = len(pda_maxtab) - 1
          pda_tide_data['HH'] = {
            'value': data_start_tag[int(maxes[max_len][0])]['WL'],
            'date':  data_start_tag[int(maxes[max_len][0])]['timeStamp']
          }
          if max_len > 0:
            pda_tide_data['H'] = {
              'value': data_start_tag[int(maxes[max_len-1][0])]['WL'],
              'date':  data_start_tag[int(maxes[max_len-1][0])]['timeStamp']
            }

        if len(pda_mintab):
          mins = sorted(pda_mintab, key=lambda rec: rec[1], reverse=True)
          max_len = len(pda_mintab) - 1
          pda_tide_data['LL'] = {
            'value': data_start_tag[int(mins[max_len][0])]['WL'],
            'date':  data_start_tag[int(mins[max_len][0])]['timeStamp']
          }
          if max_len > 0:
            pda_tide_data['L'] = {
              'value': data_start_tag[int(mins[max_len-1][0])]['WL'],
              'date':  data_start_tag[int(mins[max_len-1][0])]['timeStamp']
            }
        tide_stage = self.calc_tide_stage(wlData, beginDate, endDate, pytz_timezone('UTC'), 10, True)
        pda_tide_data['tide_stage'] = tide_stage
      except Exception as e:
        if self.logger:
          self.logger.exception(e)

    return pda_tide_data


  def calc_tide_stage(self,
                      wlData,
                      begin_date,
                      end_date,
                      tz_obj,
                      hours_buffer,
                      write_tide_stage_debug=False):
    try:
      tide_stage = -9999
      tide_data = { 'LL': None,
      'HH': None,
      'L': None,
      'H': None,
      }

      start_time_ndx = end_date - timedelta(hours=hours_buffer)
      end_time_ndx = end_date + timedelta(hours=hours_buffer)
      start_ndx = None
      end_ndx = None


      #It's seemingly impossible to use object notation to navigate to the data.
      data_start_tag = wlData.Body.getchildren()[0].getchildren()[0].item
      dataLen = len(data_start_tag)
      #Get the previous 24 hours of data we are interested in.
      for ndx in range(0, dataLen):
        wl_time = tz_obj.localize(datetime.strptime(data_start_tag[ndx]['timeStamp'].text, '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= start_time_ndx:
          start_ndx = ndx
        if end_ndx is None and wl_time > end_time_ndx:
          end_ndx = ndx-1

      tide_recs = data_start_tag[start_ndx:end_ndx]
      recs = [tide_recs[ndx]['WL'] for ndx, data in enumerate(tide_recs)]
      pda_maxtab, pda_mintab = pda_peakdetect(recs, None, 10, 0, False)

      max_len = len(pda_maxtab) - 1
      tide_data['HH'] = {
        'value': tide_recs[int(pda_maxtab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_maxtab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['H'] = {
          'value': tide_recs[int(pda_maxtab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_maxtab[max_len-1][0])]['timeStamp']
        }
      max_len = len(pda_mintab) - 1
      tide_data['LL'] = {
        'value': tide_recs[int(pda_mintab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_mintab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['L'] = {
          'value': tide_recs[int(pda_mintab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_mintab[max_len-1][0])]['timeStamp']
        }
      tide_levels = ['H','HH', 'L', 'LL']
      tide_changes = [tide_data[tide_level] for tide_level in tide_levels if tide_level in tide_data and tide_data[tide_level] is not None]
      tide_changes = sorted(tide_changes, key=lambda k: k['date'])

      #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
      #the 2000 for flood or 4000 for ebb.
      tide_stages = [0, 100, 200, 300]
      prev_tide_data_rec = None
      tolerance = timedelta(hours = 1)
      for tide_sample in tide_changes:
        if prev_tide_data_rec is not None:
          prev_date_time = tz_obj.localize(datetime.strptime(str(prev_tide_data_rec['date']), '%Y-%m-%d %H:%M:%S.0'))
          cur_date_time = tz_obj.localize(datetime.strptime(str(tide_sample['date']), '%Y-%m-%d %H:%M:%S.0'))
          if (end_date >= prev_date_time - tolerance or end_date >= prev_date_time + tolerance)\
            and (end_date < cur_date_time - tolerance or end_date < cur_date_time + tolerance):
            prev_level = float(prev_tide_data_rec['value'])
            cur_level = float(tide_sample['value'])
            if prev_level < cur_level:
              tide_state = 2000
            else:
              tide_state = 4000

            #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
            #up into 4 pieces, then figure out where our query time falls.
            time_delta = cur_date_time - prev_date_time
            qtr_time = time_delta.total_seconds() / 4.0
            prev_time = prev_date_time
            for i in range(0, 4):
              if end_date >= prev_time and end_date < (prev_time + timedelta(seconds=qtr_time)):
                 tide_stage = tide_state + tide_stages[i]
                 break

              prev_time = prev_time + timedelta(seconds=qtr_time)

        if tide_stage != -9999:
          break
        prev_tide_data_rec = tide_sample
    except Exception as e:
      if self.logger:
        self.logger.exception(e)

    if write_tide_stage_debug:
      try:
        with open('/Users/danramage/tmp/tide_stage/%s.csv' % (end_date.strftime('%Y-%m-%d_%H_%M')), 'w') as tide_data_out:
          for rec in tide_recs:
            tide_data_out.write("%s,%f\n" % (rec['timeStamp'], rec['WL']))

      except IOError as e:
        if self.logger:
          self.logger.exception(e)

    return tide_stage

  def get_tide_stage(self, begin_date,
                            end_date,
                            station,
                            datum='MLLW',
                            units='feet',
                            time_zone='GMT',
                            write_tide_data=False):
    tide_data = { 'LL': None,
    'HH': None,
    'L': None,
    'H': None,
    'PeakValue': None,
    'ValleyValue': None,
    'tide_stage': None}
    tide_stage = -9999
    try:
      if self.use_raw:
        wlData = self.getWaterLevelRawSixMinuteData(begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'), station, datum, units, time_zone)
      else:
        wlData = self.getWaterLevelVerifiedSixMinuteData(begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'), station, datum, units, time_zone)
    except (WebFault,Exception) as e:
      if self.logger:
        self.logger.exception(e)

    tz_obj = None
    if time_zone == 'GMT':
      tz_obj = pytz_timezone('UTC')

    #if begin_time == tz_obj.localize(datetime.strptime('2001-08-27 04:00:00', '%Y-%m-%d %H:%M:%S')):
    #  i = 0
    try:
      start_time_ndx = end_date - timedelta(hours=10)
      end_time_ndx = end_date + timedelta(hours=10)
      start_ndx = None
      end_ndx = None
      for ndx in range(0, len(wlData.item)):
        wl_time = tz_obj.localize(datetime.strptime(wlData.item[ndx]['timeStamp'], '%Y-%m-%d %H:%M:%S.0'))
        if start_ndx is None and wl_time >= start_time_ndx:
          start_ndx = ndx
        if end_ndx is None and wl_time > end_time_ndx:
          end_ndx = ndx-1

      #tide_recs = wlData.item[start_ndx:end_ndx]
      tide_recs = wlData.item[start_ndx:end_ndx]
      #self.find_tide_change_points(tide_recs, chordLen, tide_data)

      recs = [tide_recs[ndx]['WL'] for ndx, data in enumerate(tide_recs)]
      #Get RMS of data
      #maxtab, mintab = peakdet(recs, 0.08)
      pda_maxtab, pda_mintab = pda_peakdetect(recs, None, 10, 0, False)

      #zero_maxtab, zero_mintab = peakdetect_zero_crossing(y_axis=recs, x_axis=None, window=13)
      #Sort the maxs and mins by value then date.
      #max_sorted = sorted(pda_maxtab, key=lambda rec: (tide_recs[int(rec[0])]['WL'], tide_recs[int(rec[0])]['timeStamp']))
      #min_sorted = sorted(pda_mintab, key=lambda rec: (tide_recs[int(rec[0])]['WL'], tide_recs[int(rec[0])]['timeStamp']))
      max_len = len(pda_maxtab) - 1
      tide_data['HH'] = {
        'value': tide_recs[int(pda_maxtab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_maxtab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['H'] = {
          'value': tide_recs[int(pda_maxtab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_maxtab[max_len-1][0])]['timeStamp']
        }
      max_len = len(pda_mintab) - 1
      tide_data['LL'] = {
        'value': tide_recs[int(pda_mintab[max_len][0])]['WL'],
        'date':  tide_recs[int(pda_mintab[max_len][0])]['timeStamp']
      }
      if max_len > 0:
        tide_data['L'] = {
          'value': tide_recs[int(pda_mintab[max_len-1][0])]['WL'],
          'date':  tide_recs[int(pda_mintab[max_len-1][0])]['timeStamp']
        }
      tide_levels = ['H','HH', 'L', 'LL']
      tide_changes = [tide_data[tide_level] for tide_level in tide_levels if tide_level in tide_data and tide_data[tide_level] is not None]
      tide_changes = sorted(tide_changes, key=lambda k: k['date'])

      #0 is Full stage, either Ebb or Flood, 100 is 1/4, 200 is 1/2 and 300 is 3/4. Below we add either
      #the 2000 for flood or 4000 for ebb.
      tide_stages = [0, 100, 200, 300]
      prev_tide_data_rec = None
      tolerance = timedelta(hours = 1)
      for tide_sample in tide_changes:
        if prev_tide_data_rec is not None:
          prev_date_time = tz_obj.localize(datetime.strptime(prev_tide_data_rec['date'], '%Y-%m-%d %H:%M:%S.0'))
          cur_date_time = tz_obj.localize(datetime.strptime(tide_sample['date'], '%Y-%m-%d %H:%M:%S.0'))
          if (end_date >= prev_date_time - tolerance or end_date >= prev_date_time + tolerance)\
            and (end_date < cur_date_time - tolerance or end_date < cur_date_time + tolerance):
            prev_level = prev_tide_data_rec['value']
            cur_level = tide_sample['value']
            if prev_level < cur_level:
              tide_state = 2000
            else:
              tide_state = 4000

            #Now figure out if it is 0, 1/4, 1/2, 3/4 stage. We divide the time between the 2 tide changes
            #up into 4 pieces, then figure out where our query time falls.
            time_delta = cur_date_time - prev_date_time
            qtr_time = time_delta.total_seconds() / 4.0
            prev_time = prev_date_time
            for i in range(0, 4):
              if end_date >= prev_time and end_date < (prev_time + timedelta(seconds=qtr_time)):
                 tide_stage = tide_state + tide_stages[i]
                 break

              prev_time = prev_time + timedelta(seconds=qtr_time)

        if tide_stage != -9999:
          break
        prev_tide_data_rec = tide_sample

      tide_data['tide_stage'] = tide_stage
    except Exception, e:
      if self.logger:
        self.logger.exception(e)

    if write_tide_data:
      with open('/Users/danramage/tmp/florida_data/tide_stage_data/%s.csv' % (end_date.strftime('%Y-%m-%d_%H_%M')), 'w') as tide_data_out:
        for rec in tide_recs:
          tide_data_out.write("%s,%f\n" % (rec['timeStamp'], rec['WL']))

    return tide_data



if __name__ == '__main__':
  tide = noaaTideData()
  tide.calcTideRange(beginDate = '20110613',
                     endDate = '20110613',
                     station='8661070',
                     datum='MLLW',
                     units='feet',
                     timezone='Local Time',
                     smoothData=False,
                     tideFileDir="C:\\temp")