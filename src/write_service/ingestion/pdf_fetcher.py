from PyPDF2 import PdfReader
import requests
import io
import os

def extract_text_from_pdf(source: str, is_url: bool = False) -> list[str]:

    pdf_stream = None
    try:
        if is_url:
            response = requests.get(source, timeout=10)
            response.raise_for_status()
            pdf_stream = io.BytesIO(response.content)
        else:
            if not os.path.exists(source):
                raise FileNotFoundError(f"Local PDF not found: {source}")
            pdf_stream = open(source, "rb")

        reader = PdfReader(pdf_stream)

        if not reader.pages:
            raise ValueError("PDF has no pages or cannot be read.")

        text_by_page = []
        for page in reader.pages:
            text = page.extract_text()
            text_by_page.append(text or "")

        return text_by_page

    except Exception as e:
        raise Exception(f"Failed to extract text from PDF: {e}") from e

    finally:
        if pdf_stream and not pdf_stream.closed:
            pdf_stream.close()
