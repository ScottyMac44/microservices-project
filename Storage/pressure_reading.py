from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy.sql.functions import now
from base import Base
from datetime import datetime


class PressureReading(Base):
    """ Pressure Reading """

    __tablename__ = "pressure_reading"

    id = Column(Integer, primary_key=True)
    station_id = Column(String(250), nullable=False)
    barometer_id = Column(String(250), nullable=False)
    pressure = Column(Float, nullable=False)
    timestamp = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250), nullable=False)

    def __init__(self, station_id, barometer_id, pressure, timestamp, trace_id):
        """ Initializes a blood pressure reading """
        self.station_id = station_id
        self.barometer_id = barometer_id
        self.pressure = pressure
        self.timestamp = timestamp
        self.date_created = datetime.now() # Sets the date/time record is created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a temperature reading """
        dict = {}
        dict['id'] = self.id
        dict['station_id'] = self.station_id
        dict['barometer_id'] = self.barometer_id
        dict['pressure'] = self.pressure
        dict['timestamp'] = self.timestamp
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict