from sqlalchemy import create_engine
from base import Base
from temperature_reading import TemperatureReading # required for table creation/deletion
from pressure_reading import PressureReading # required for table creation/deletion
import pymysql
import mysql.connector
import yaml

with open('app_conf.yml', 'r') as f:
    conf = yaml.safe_load(f.read())
    user_data = conf['datastore']

DB_ENGINE = create_engine(
    f'mysql+pymysql://{user_data["user"]}:{user_data["password"]}@localhost:3306/readings')
Base.metadata.bind = DB_ENGINE

Base.metadata.create_all(DB_ENGINE)