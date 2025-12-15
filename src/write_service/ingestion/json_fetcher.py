"""
json_fetcher.py
This code retrieves the raw JSON data from a given URL.
These functions are used in other parts of the project.

Here is how you run my JSON fetcher, JSON processer, and JSON consumer.
This is also how ARPA data from the City of St. Louis Open Data Portal
is saved into the database:
    1. Start up the project's Docker containers.
    2. Do one of the following:
        - Go to http://localhost:5000/json. The ARPA data will be saved into the database.
        You should see a webpage displaying what was saved 
        in the database along with the Kafka status. The PostgreSQL 
        application, if connected properly to the project, should also display the table data.

        - OR run python -m src.write_service.consumers.json_consumer from the project's root folder. 
        The ARPA data will be saved into the database. The terminal should display what was 
        received from Kafka and what was inserted into the database. The PostgreSQL application, 
        if connected properly to the project, should also display the table data.
"""
import requests
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_json(url):
    """
    This functions grabs the JSON data from a URL and then parses it.
    """

    try:
        # Get the JSON data
        response = requests.get(url)
        response.raise_for_status()

        # Parse the data
        data = response.json()

        # Return the data
        logger.info(f"Data received successfully from {url}: \n {data}")
        return data

    except requests.exceptions.HTTPError as httpError:
        code = httpError.response.status_code
        error = httpError

        # Handle HTTP errors
        if code == 404:
            error = f"Error 404: Unable to get JSON from {url} because page not found. \nError: {httpError}"
        elif code == 500:
            error = f"Error 500: Unable to get JSON from {url} because the other server is not working. \nError: {httpError}"
        else:
            error = f"Unknown HTTP Error {code}: Unable to get JSON from {url}. \nError: {httpError}"

        logger.error(error)
        return error

    except json.JSONDecodeError as jsonError:
        # If JSON data is not valid
        error = f"The JSON data is not valid. \nError: {jsonError}"
        logger.error(error)
        return error
    
    except requests.exceptions.RequestException as requestError:
        # If something wrong with the connection
        error = f"Something went wrong with the connection. \nError: {requestError}"
        logger.error(error)
        return error
