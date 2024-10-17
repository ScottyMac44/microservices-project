import connexion
import requests
import logging
import logging.config
import time
import yaml
from connexion import NoContent
import os.path

with open('/home/spmcneill/microservices_project/Reciever/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/home/spmcneill/microservices_project/Reciever/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
    logger = logging.getLogger('basicLogger') # Move outside with block if causing problems


def report_temperature_reading(body):
    trace_id = time.time()
    logger.info(f"Recieved event report_temperature_reading request with a trace ID of {str(trace_id)}")
    body['trace_id'] = str(trace_id)

    response = requests.post(
        url     = app_config['eventstore1']['url'],
        json    = body,
        headers = {
            "Content_Type": "application/json"
            }
        )
    
    logger.info(f"Returned event report_temperature_reading response (ID: {str(trace_id)}) with status {response.status_code}")

    return response.text, response.status_code

def report_pressure_reading(body):
    trace_id = time.time()
    logger.info(f"Recieved event report_pressure_reading request with a trace ID of {str(trace_id)}")
    body['trace_id'] = str(trace_id)

    response = requests.post(
        url     = app_config['eventstore2']['url'],
        json    = body,
        headers = {
            "Content_Type": "application/json"
            }
        )
    
    logger.info(f"Returned event report_pressure_reading response (ID: {str(trace_id)}) with status {response.status_code}")

    return response.text, response.status_code

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    print("Endpoint testing at http://localhost:8080/ui")
    app.run(port=8080)