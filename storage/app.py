import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from base import Base
from pressure_reading import PressureReading
from temperature_reading import TemperatureReading
import datetime
import pymysql
import mysql.connector
import yaml
import logging
import logging.config

with open('Storage/app_conf.yml', 'r') as f:
    conf = yaml.safe_load(f.read())
    dbdata = conf['datastore']

with open('/home/spmcneill/microservices_project/Storage/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
    logger = logging.getLogger('basicLogger') # Move outside with block if causing problems

DB_ENGINE = create_engine(
    f'mysql+pymysql://{dbdata["user"]}:{dbdata["password"]}@{dbdata["hostname"]}:{dbdata["port"]}/{dbdata["db"]}')

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def report_temperature_reading(body):
    """ Recieves a temperature reading """
    
    session = DB_SESSION()

    session.add(TemperatureReading(
        body['station_id'],
        body['thermometer_id'],
        body['temperature'],
        body['timestamp'],
        body['trace_id']
    ))

    session.commit()
    session.close()

    logger.debug(f"Stored event temperature_reading with a trace ID of {body['trace_id']}")

    return NoContent, 201


def report_pressure_reading(body):
    """ Recieves a pressure reading """
    
    session = DB_SESSION()

    session.add(PressureReading(
        body['station_id'],
        body['barometer_id'],
        body['pressure'],
        body['timestamp'],
        body['trace_id']
    ))

    session.commit()
    session.close()
    
    logger.debug(f"Stored event pressure_reading with a trace ID of {body['trace_id']}")

    return NoContent, 201

def get_temperature_reading(start_timestamp, end_timestamp):
    """ Gets new temperature readings between the start and end timestamps """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")

    results = session.query(TemperatureReading).filter(
        and_ (TemperatureReading.date_created >= start_timestamp_datetime,
        TemperatureReading.date_created < end_timestamp_datetime))
    
    results_list = []

    for reading in results:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for Temperature readings after %s returns %d results" %(start_timestamp, len(results_list)))

    return results_list, 200

def get_pressure_reading(start_timestamp, end_timestamp):
    """ Gets new pressure readings between the start and end timestamps """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp.split(".")[0], "%Y-%m-%dT%H:%M:%S")

    results = session.query(PressureReading).filter(
        and_ (PressureReading.date_created >= start_timestamp_datetime,
        PressureReading.date_created < end_timestamp_datetime))
    
    results_list = []

    for reading in results:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for Pressure readings after %s returns %d results" %(start_timestamp_datetime, len(results_list)))

    return results_list, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    print("Starting storage service")
    app.run(port=8090)
