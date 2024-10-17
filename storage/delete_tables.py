from sqlalchemy import create_engine
from base import Base
from temperature_reading import TemperatureReading # required for table creation/deletion
from pressure_reading import PressureReading # required for table creation/deletion

DB_ENGINE = create_engine("sqlite:///readings.sqlite")
Base.metadata.bind = DB_ENGINE

Base.metadata.drop_all(DB_ENGINE)