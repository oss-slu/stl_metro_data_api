import json
import logging
from flask import Flask, render_template, render_template_string
from src.write_service.ingestion.json_fetcher import get_json
from src.write_service.processing.json_processor import send_data
from src.write_service.consumers.json_consumer import retrieve_from_kafka, save_into_database

# This is the Python app for the WRITE service
app = Flask(__name__)

@app.route('/')
def main():
    """For now, we just show a simple webpage."""
    logging.info("The root directory of the write service has been accessed!")
    return render_template("index.html")

@app.route('/json', strict_slashes = False)
def test_json():
    """
    Test function that pulls sample JSON data from the City of St. Louis website, parses it,
    sends it to Kafka, then retrieves from Kafka, and finally saves into the database.
    Results and messages are displayed the results to the user.
    This function tests the JSON fetcher, JSON processor, and JSON consumer.
    Make sure Docker containers are up and running first.
    Go to http://localhost:5000/json to see it for yourself!
    """

    # Grab and parse data from URL, also send to Kafka
    testURL = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(testURL)
    kafka_status = send_data(result, "arpa")
    result2 = retrieve_from_kafka("arpa")
    save_into_database(result2, "ARPA_funds", "ARPA funds usage")

    # Display results to user
    formattedResult = json.dumps(result2, indent=2)
    html = f"""
        <html>
            <head>
                <title>STL Data API - Write Service - JSON Test</title>
            </head>
            <body>
                <h1>STL Data API - Write Service - JSON Test</h1>
                <h2>JSON from {testURL}</h2>
                <p>
                    <b>Kafka Status:</b><br>
                    {kafka_status}
                <hr><br>
                <b>JSON data saved into the database:</b>
                <pre>{formattedResult}</pre>
                </p>
            </body>
        </html>
    """
    return render_template_string(html)

@app.route('/health')
def health():
    """Endpoint for checking health of this app (if basic endpoint works or not)."""
    logging.info("Health is okay.")
    return {'status': 'ok'}

if __name__ == '__main__':
    """Called when this app is started."""
    logging.info("The write service Python app has started.")
    app.run(host='0.0.0.0', port=5000)
