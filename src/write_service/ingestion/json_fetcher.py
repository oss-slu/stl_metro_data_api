import requests
import logging
import json

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
        logging.info(f"Data received successfully from {url}: \n {data}")
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

        logging.error(error)
        return error

    except json.JSONDecodeError as jsonError:
        # If JSON data is not valid
        error = f"The JSON data is not valid. \nError: {jsonError}"
        logging.error(error)
        return error
    
    except requests.exceptions.RequestException as requestError:
        # If something wrong with the connection
        error = f"Something went wrong with the connection. \nError: {requestError}"
        logging.error(error)
        return error