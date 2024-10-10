from apscheduler.schedulers.background import BackgroundScheduler
import connexion
import requests
import swagger_ui_bundle
import apscheduler
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

def populate_stats():
    logger.info("Starting stats processing job")

    try:
        with open(app_config['datastore']['filename'], 'r') as f:
            stats = json.loads(f.read())

    except FileNotFoundError:
        stats = {
            "mean_temperature": 0,
            "mean_pressure": 0,
            "num_temperature_readings": 0,
            "num_pressure_readings": 0,
            "last_updated": datetime.datetime.now().isoformat() - datetime.timedelta(seconds=app_config['scheduler']['period_sec'])
        } # Default values

    finally:
        end_timestamp = datetime.datetime.now()
        start_timestamp = stats['last_updated']

        temperature_response = requests.get(f"{app_config['eventstore']['url']}/requests/temperature",
                                            params={"start_timestamp": start_timestamp, "end_timestamp": end_timestamp})
        pressure_response = requests.get(f"{app_config['eventstore']['url']}/requests/pressure",
                                        params={"start_timestamp": start_timestamp, "end_timestamp": end_timestamp})
        
        if temperature_response.status_code != 200 or pressure_response.status_code != 200:
            logger.error("Failed to retrieve data from storage service")
            return

        temperatures = [entry['temperature'] for entry in temperature_response.json()['body']]
        mean_temperature = sum(temperatures) / len(temperatures) if temperatures else 0

        pressures = [entry['pressure'] for entry in pressure_response.json()['body']]
        mean_pressure = sum(pressures) / len(pressures) if pressures else 0

        num_temperature_readings = len(temperatures)
        num_pressure_readings = len(pressures)

        logger.info(f"Recieved {num_temperature_readings + num_pressure_readings} readings")

        stats = {
            "mean_temperature": mean_temperature,
            "mean_pressure": mean_pressure,
            "num_temperature_readings": num_temperature_readings,
            "num_pressure_readings": num_pressure_readings,
            "last_updated": end_timestamp.isoformat()
        }

        with open(app_config['datastore']['filename'], 'w') as f:
            f.write(json.dumps(stats, indent=4))

        logger.info("Stats processing job completed")

        return stats


def get_stats():

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
