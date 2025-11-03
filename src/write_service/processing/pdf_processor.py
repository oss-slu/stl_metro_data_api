from __future__ import annotations
import re
import json
import gc
from typing import Dict, Any, List, Optional, Sequence, Tuple

try:
    from src.write_service.ingestion import pdf_fetcher  # type: ignore
except Exception:
    try:
        from ..ingestion import pdf_fetcher
    except Exception:
        import pdf_fetcher  # type: ignore


DATE_PATTERNS = [
    r'\b\d{4}-\d{2}-\d{2}\b',  # dates: yyyy-mm-dd
    r'\b\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b',  # dates: dd/mm/yyyy or dd-mm-yyyy or d/m/yy
    
    # month names (month dd, yyyy or mon dd yyyy)
    r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|'
    r'May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|'
    r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2}(?:,\s*\d{4})?\b',
]

NAME_PATTERN = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z\.]+){0,2})\b'

COMMON_NON_NAMES = {
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November",
    "December", "Jan", "Feb", "Mar", "Apr", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    "The", "A", "An", "In", "On", "At", "For", "And", "But", "Or", "By", "To", "From", "With"
}


def clean_text(text: str) -> str:
    t = re.sub(r'[\r\n\t]+', ' ', text)  # replace control characters with spaces
    t = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+', ' ', t)  # remove non-printable characters
    t = re.sub(r'\s+', ' ', t)  # remove whitespace
    return t.strip()


def extract_entities(text: str) -> Dict[str, List[str]]:
    dates: List[str] = []
    for pat in DATE_PATTERNS:
        for m in re.finditer(pat, text):
            v = m.group(0).strip()
            if v not in dates:
                dates.append(v)

    raw_names: List[str] = []
    for m in re.finditer(NAME_PATTERN, text):
        candidate = m.group(1).strip()
        if len(candidate) < 2:
            continue
        if candidate in COMMON_NON_NAMES:
            continue
        if re.fullmatch(r'\d{2,4}', candidate):  # looks like a year
            continue
        if candidate not in raw_names:
            raw_names.append(candidate)

    return {"dates": dates, "names": raw_names}


def _split_table_row(line: str) -> List[str]:
    if '|' in line:
        parts = [c.strip() for c in line.split('|') if c.strip() != ""]
        if len(parts) > 1:
            return parts

    if '\t' in line:
        parts = [c.strip() for c in line.split('\t') if c.strip() != ""]
        if len(parts) > 1:
            return parts

    parts = [p.strip() for p in re.split(r'\s{2,}', line) if p.strip() != ""]
    return parts


def parse_tables_from_text(text: str) -> List[List[List[str]]]:
    lines = [ln.rstrip() for ln in text.splitlines()]
    tables: List[List[List[str]]] = []
    current_table: List[List[str]] = []

    for line in lines:
        if not line.strip():
            if len(current_table) >= 2:
                tables.append(current_table)
            current_table = []
            continue

        cells = _split_table_row(line)
        if len(cells) >= 2:
            current_table.append(cells)
        else:
            if len(current_table) >= 2:
                tables.append(current_table)
            current_table = []

    if len(current_table) >= 2:
        tables.append(current_table)

    return tables


# for kafka
def build_payload(pdf_id: str, entities: Dict[str, Any], snippet: Optional[str] = None,
                  tables: Optional[List[List[List[str]]]] = None) -> bytes:
    payload = {
        "pdf_id": pdf_id,
        "entities": entities,
        "snippet": (snippet or "")[:300],
        "tables": tables or [],
        "schema_version": 1,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _send_payload_bytes(producer: Any, topic: str, payload_bytes: bytes) -> Any:
    if hasattr(producer, "send"):
        return producer.send(topic, payload_bytes)
    elif hasattr(producer, "produce"):
        return producer.produce(topic, payload_bytes)
    elif callable(producer):
        return producer(topic, payload_bytes)
    else:
        raise AttributeError("Producer does not have a supported interface (.send/.produce/callable)")


def process_and_send(pdf_id: str, raw_bytes: Any, producer: Any, topic: str = "pdf-processed-topic") -> Dict[str, Any]:
    is_bytearray = isinstance(raw_bytes, bytearray)
    is_bytes = isinstance(raw_bytes, (bytes, bytearray))
    is_str = isinstance(raw_bytes, str)

    if is_bytearray:
        text = raw_bytes.decode("utf-8", errors="ignore")
    elif is_bytes:
        text = raw_bytes.decode("utf-8", errors="ignore")
    elif is_str:
        text = raw_bytes
    else:
        text = str(raw_bytes)

    cleaned = clean_text(text)
    entities = extract_entities(cleaned)
    tables = parse_tables_from_text(text)
    snippet = cleaned[:300]

    payload_bytes = build_payload(pdf_id, entities, snippet, tables)

    send_successful = False
    send_result = None
    try:
        send_result = _send_payload_bytes(producer, topic, payload_bytes)
        send_successful = True
    finally:
        try:
            if is_bytearray:
                for i in range(len(raw_bytes)):
                    raw_bytes[i] = 0
            else:
                del text
                del cleaned
                del payload_bytes
                gc.collect()
        except Exception:
            pass

    return {
        "pdf_id": pdf_id,
        "entities": entities,
        "snippet": snippet,
        "tables_count": len(tables),
        "sent": bool(send_successful),
        "send_result": send_result,
    }


def process_pdf_file(pdf_id: str, source: str, producer: Any, topic: str = "pdf-processed-topic",
                     is_url: bool = False) -> Dict[str, Any]:
    if not is_url and (source.startswith("http://") or source.startswith("https://")): 
        is_url = True

    pages = pdf_fetcher.extract_text_from_pdf(source, is_url=is_url)
    full_text = "\n".join(pages)
    
    return process_and_send(pdf_id, full_text, producer, topic)