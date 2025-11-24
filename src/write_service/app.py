import json
import logging
from flask import Flask, render_template, render_template_string
from src.write_service.ingestion.json_fetcher import get_json
from src.write_service.processing.json_processor import send_data

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
    sends it to Kafka, and then displays the results to the user.
    """

    # Grab and parse data from URL, also send to Kafka
    testURL = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(testURL)
    kafka_status = send_data(result)

    # Display results to user
    formattedResult = json.dumps(result, indent=2)
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
                <b>JSON data received from website (not cleaned yet):</b>
                <pre>{formattedResult}</pre>
                </p>
            </body>
        </html>
    """
    return render_template_string(html)

@app.route('/pdf', strict_slashes=False)
def test_pdf():
    """
    Test endpoint that:
    - fetches a PDF (from URL or local path),
    - extracts text,
    - processes tables/entities,
    - sends processed payload to Kafka,
    - and displays results in the browser.
    """

    from src.write_service.ingestion.pdf_fetcher import extract_text_from_pdf
    from src.write_service.processing.pdf_processor import process_pdf_file

    # Example PDF source (change to anything you want)
    pdf_id = "test_pdf_001"
    pdf_source = "https://www.stlouis-mo.gov/government/departments/human-services/homeless-services/documents/upload/Revised-2012-ESG-Action-Plan.pdf"
    is_url = True

    try:
        # ---- Extract text pages for display ----
        pages = extract_text_from_pdf(pdf_source, is_url=is_url)

        # ---- Process PDF (entities, tables, snippet) and send to Kafka ----
        kafka_status = process_pdf_file(
            pdf_id=pdf_id,
            source=pdf_source,
            producer=send_data,     # your existing Kafka producer wrapper
            topic="pdf-processed-topic",
            is_url=is_url
        )

        # ---- Build UI Output ----
        formatted_pages = "<br><hr><br>".join(
            f"<h3>Page {i+1}</h3><pre>{p}</pre>"
            for i, p in enumerate(pages)
        )

        html = f"""
            <html>
                <head>
                    <title>PDF Test - WRITE Service</title>
                </head>
                <body>
                    <h1>PDF Extraction + Processing Test</h1>

                    <h2>Source PDF:</h2>
                    <p>{pdf_source}</p>

                    <h2>Kafka Processing Result:</h2>
                    <pre>{json.dumps(kafka_status, indent=2)}</pre>

                    <h2>Extracted PDF Text by Page:</h2>
                    {formatted_pages}
                </body>
            </html>
        """

        return render_template_string(html)

    except Exception as e:
        logging.error("PDF test failed", exc_info=True)
        return {"error": "An internal error occurred while processing the PDF."}, 500


@app.route('/health')
def health():
    """Endpoint for checking health of this app (if basic endpoint works or not)."""
    logging.info("Health is okay.")
    return {'status': 'ok'}

if __name__ == '__main__':
    """Called when this app is started."""
    logging.info("The write service Python app has started.")
    app.run(host='0.0.0.0', port=5000)