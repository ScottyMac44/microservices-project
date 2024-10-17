from sqlalchemy import create_engine
from base import Base
from temperature_reading import TemperatureReading # required for table creation/deletion
from pressure_reading import PressureReading # required for table creation/deletion
import pymysql
import mysql.connector
import yaml

with open('Storage/app_conf.yml', 'r') as f:
    conf = yaml.safe_load(f.read())
    dbdata = conf['datastore']

DB_ENGINE = create_engine(
    f'mysql+pymysql://{dbdata["user"]}:{dbdata["password"]}@{dbdata["hostname"]}:{dbdata["port"]}/{dbdata["db"]}')

Base.metadata.bind = DB_ENGINE

Base.metadata.drop_all(DB_ENGINE)