import pytest
import sys
import pathlib

from write_service.ingestion.pdf_parser import extract_text_from_pdf

def test_extract_text_from_local_pdf():
    dummy_pdf_path = "tests/write_service/dummy.pdf"
    text = extract_text_from_pdf(dummy_pdf_path)
    assert isinstance(text, list)
    assert len(text) > 0
    assert any(t.strip() for t in text)

def test_extract_text_from_url_pdf():
    url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
    text = extract_text_from_pdf(url, is_url=True)
    assert isinstance(text, list)
    assert len(text) > 0
    assert any(t.strip() for t in text)

def test_local_file_not_found():
    with pytest.raises(Exception, match="Local PDF not found"):
        extract_text_from_pdf("nonexistent_file.pdf")

def test_invalid_url():
    with pytest.raises(Exception):
        extract_text_from_pdf("http://invalid.url/doesnotexist.pdf", is_url=True)