import pytest
import sys
import pathlib
import json

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from src.write_service.processing import pdf_processor

class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, payload_bytes):

        self.sent.append((topic, payload_bytes))

        return {"status": "ok", "topic": topic, "len": len(payload_bytes)}


def test_clean_and_extract_entities():
    raw = "This document was created on 2023-08-15. Contact: John Doe. Another date: 12/31/2022."
    cleaned = pdf_processor.clean_text(raw)
    assert "  " not in cleaned
    ents = pdf_processor.extract_entities(cleaned)
    assert "2023-08-15" in ents["dates"]
    assert "12/31/2022" in ents["dates"]
    assert any("John Doe" in n for n in ents["names"])


def test_parse_tables_from_text_pipe_and_spaces():
    text = (
        "Header1 | Header2 | Header3\n"
        "val1 | val2 | val3\n"
        "\n"
        "Name   Age   Score\n"
        "Alice  30    98\n"
        "Bob    25    88\n"
    )
    tables = pdf_processor.parse_tables_from_text(text)

    assert len(tables) == 2

    assert tables[0][0][0].lower().startswith("header1")
    assert tables[1][1][0].lower().startswith("alice")


def test_process_and_send_with_fake_producer_and_bytearray_cleanup():
    fake = FakeProducer()
    pdf_id = "test-doc-1"
    text = "Sample text with date 2021-01-01 and Person Name Example Person.\nCol1 | Col2\nA | B\n"

    raw = bytearray(text.encode("utf-8"))
    result = pdf_processor.process_and_send(pdf_id, raw, fake, topic="pdf-test-topic")
    assert result["pdf_id"] == pdf_id
    assert result["sent"] is True
    assert result["tables_count"] >= 1

    assert len(fake.sent) == 1
    topic_sent, payload_bytes = fake.sent[0]
    assert topic_sent == "pdf-test-topic"
    payload = json.loads(payload_bytes.decode("utf-8"))
    assert payload["pdf_id"] == pdf_id

    assert all(b == 0 for b in raw)

@pytest.fixture
def dummy_pdf_path():
    return "tests/fixtures/dummy-tables.pdf"

def test_process_pdf_file_with_dummy_pdf(dummy_pdf_path):
    fake = FakeProducer()
    res = pdf_processor.process_pdf_file("pdf-dummy-1", dummy_pdf_path, fake, topic="pdf-dummy-topic")

    assert res["pdf_id"] == "pdf-dummy-1"
    assert res["sent"] is True
    assert res["tables_count"] >= 0

    topic_sent, payload_bytes = fake.sent[0]
    assert topic_sent == "pdf-dummy-topic"

    payload = json.loads(payload_bytes.decode("utf-8"))
    assert payload["pdf_id"] == "pdf-dummy-1"
    assert isinstance(payload.get("entities"), dict)

    assert isinstance(payload.get("entities"), dict)

@pytest.fixture
def dummy_pdf_web_path():
    return "https://assets.accessible-digital-documents.com/uploads/2017/01/sample-tables.pdf"


def test_process_pdf_file_with_dummy_pdf_web(dummy_pdf_web_path):
    fake = FakeProducer()
    pdf_url = dummy_pdf_web_path.strip()  # Always strip trailing/leading spaces

    res = pdf_processor.process_pdf_file(
        "pdf-dummy-1",
        pdf_url,
        fake,
        topic="pdf-dummy-topic",
        is_url=True,
    )

    assert res["pdf_id"] == "pdf-dummy-1"
    assert res["sent"] is True
    assert res["tables_count"] >= 0

    topic_sent, payload_bytes = fake.sent[0]
    assert topic_sent == "pdf-dummy-topic"

    payload = json.loads(payload_bytes.decode("utf-8"))
    assert payload["pdf_id"] == "pdf-dummy-1"
    assert isinstance(payload.get("entities"), dict)