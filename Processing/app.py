from apscheduler.schedulers.background import BackgroundScheduler
from connexion import NoContent
import connexion
import requests
import swagger_ui_bundle # Required for swagger endpoint testing UI
import logging
import logging.config
import yaml
import json
import datetime

with open('Processing/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('Processing/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
    logger = logging.getLogger('basicLogger')

# Private function to load datastore, make requests to storage service,
# calculate new stats, and return updated stats and end_timestamp
def _generate_stats(start_param=None, end_param=None):
    # Attempt to load stats datastore
    try:
        with open(app_config['datastore']['filename'], 'r') as f:
            stats = json.loads(f.read())

    except FileNotFoundError: # Generate default values if file does not exist
        logger.error("File not found")
        return (NoContent, None, 404)
        # Uncomment below to automatically handle the datastore file not existing
        # logger.warning(f"File {app_config['datastore']['filename']} not found. Creating new file and using default values")
        # last_updated = (datetime.datetime.now() - datetime.timedelta(seconds=app_config['scheduler']['period_sec'])).isoformat()
        # stats = {
        #     "mean_temperature": 0,
        #     "mean_pressure": 0,
        #     "num_temperature_readings": 0,
        #     "num_pressure_readings": 0,
        #     "last_updated": str(last_updated)
        # }
        # # Create new file and write defaults to it
        # logger.info("Writing default values to datastore")
        # with open(app_config['datastore']['filename'], 'w') as f:
        #     json.dump(stats, f, indent=4)

    finally:
        start_timestamp = stats['last_updated']
        end_timestamp = datetime.datetime.now().isoformat()

        # Make GET requests to storage service temperature/pressure endpoints
        logger.info("Making requests to storage endpoints")
        temperature_response = requests.get(f"{app_config['eventstore']['url']}/readings/temperature",
                                            params={"start_timestamp": str(start_timestamp), "end_timestamp": str(end_timestamp)},
                                            timeout=1)
        pressure_response = requests.get(f"{app_config['eventstore']['url']}/readings/pressure",
                                        params={"start_timestamp": str(start_timestamp), "end_timestamp": str(end_timestamp)},
                                        timeout=1)
        logger.info("Got responses from storage endpoints")
        
        # If either request fails, log an error and return the last saved stats
        if temperature_response.status_code != 200 or pressure_response.status_code != 200:
            logger.warning(f"Failed to retrieve data from storage service. temp status code: {temperature_response.status_code}, pressure status code: {pressure_response.status_code}")
            return stats
        
        # Calculate new stats
        logger.info("Starting stats processing job")
        temperatures = [entry['temperature'] for entry in temperature_response.json()]
        mean_temperature = sum(temperatures) / len(temperatures) if temperatures else 0

        pressures = [entry['pressure'] for entry in pressure_response.json()]
        mean_pressure = sum(pressures) / len(pressures) if pressures else 0

        num_temperature_readings = len(temperatures)
        num_pressure_readings = len(pressures)

        # Update stats dict with new stats
        stats = {
            "mean_temperature": mean_temperature,
            "mean_pressure": mean_pressure,
            "num_temperature_readings": stats['num_temperature_readings'] + num_temperature_readings,
            "num_pressure_readings": stats['num_pressure_readings'] + num_pressure_readings
        } # Exclude last_updated because it is not specified in get_stats() OpenAPI definition
        logger.debug(f"Updated stats: {stats}")
        logger.info(f"Stats processing job completed. Recieved a total of {num_temperature_readings + num_pressure_readings} readings")

        return stats, end_timestamp, 200

# Private function which updates the datastore with newly calculated stats
def _populate_stats():
    stats, last_updated, code = _generate_stats()
    if code != 404: # Do nothing if data.json not found
        stats['last_updated'] = last_updated
        with open(app_config['datastore']['filename'], 'w') as f:
            json.dump(stats, f, indent=4)

# Private function which creates the populate_stats background job
def _init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(_populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

# Public function which serves as the API's GET endpoint
def get_stats():
    generated_stats = _generate_stats()
    stats = generated_stats[0]
    code = generated_stats[2]
    return stats, code

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    _init_scheduler()
    app.run(port=8100)
    logger.info("Endpoint testing at http://localhost:8100/ui")
