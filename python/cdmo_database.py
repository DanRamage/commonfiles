from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Float, Boolean, func, Text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
#from sqlalchemy.orm import eagerload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy import exc
from sqlalchemy.orm.exc import *

import logging.config

Base = declarative_base()

"""
ERDS DAtabase
"""

class station_dim(Base):
  __tablename__ = 'StationDim'
  ID = Column(Integer, primary_key=True, autoincrement=False)
  NERR_Site_ID = Column(String(50))
  Station_Code = Column(String(10))
  Station_Name = Column(String(40))
  Lat_Long = Column(String(50))
  Latitude = Column(String(50))
  Longitude = Column(String(50))
  Status = Column(String(10))
  Active_Dates  = Column(String(50))
  Real_Time = Column(String(10))
  HADS_ID  = Column(String(10))
  GMT_Offset = Column(String(10))
  Station_Type = Column(Integer)
  Region = Column(Integer)
  Params_Reported = Column(String(300))
  Report_Errors = Column(Boolean)
  NERR_Site_Code = Column(String(10))
  NERR_Site_Name = Column(String(40))
  State = Column(String(10))
  IOOS_shortname = Column(String(40))
  NOAA_Name = Column(String(25))
  location_fk = Column(Integer)



"""
CDMO database
"""
class web_services_tracking(Base):
  __tablename__ = 'webservicesTracking'
  DateTimeStamp    = Column(DateTime)
  ip               = Column(String(50))
  stationCode     = Column(String(10))
  functionCalled   = Column(String(100))
  paramCalled      = Column(String(500))
  dateOne          = Column(String(50))
  dateTwo          = Column(String(50))
  recs             = Column(Integer)
  id               = Column(Integer, primary_key=True)

class mobile_web_services_tracking(Base):
  __tablename__ = 'mobileTracking'
  id               = Column(Integer, primary_key=True)
  stationCode      = Column(String(50))
  DateTimeStamp    = Column(DateTime)
  osInfo           = Column(Text())

class StationBatteryVoltage(Base):
  __tablename__ = 'StationBatteryVoltage'

  ID = Column(Integer, primary_key=True)
  DateTimeStamp = Column(DateTime)
  SamplingStation = Column(String(16))
  Voltage = Column(Float())


class cdmo_sqlalchemy(object):
  def __init__(self, use_logging=True):
    self.dbEngine = None
    self.metadata = None
    self.session  = None
    self.logger   = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

  def connectDB(self, connect_string, printSQL = False):

    try:
      #Connect to the database
      self.dbEngine = create_engine(connect_string, echo=printSQL)

      #metadata object is used to keep information such as datatypes for our table's columns.
      self.metadata = MetaData()
      self.metadata.bind = self.dbEngine

      Session = sessionmaker(bind=self.dbEngine)
      self.session = Session()

      self.connection = self.dbEngine.connect()

      return(True)
    except (exc.OperationalError, exc.InterfaceError) as e:
      if self.logger:
        self.logger.exception(e)
    return(False)

  def disconnect(self):
    self.session.close()
    self.connection.close()
    self.dbEngine.dispose()

  def get_count(self, q):
      count_q = q.statement.with_only_columns([func.count()]).order_by(None)
      count = q.session.execute(count_q).scalar()
      return count
