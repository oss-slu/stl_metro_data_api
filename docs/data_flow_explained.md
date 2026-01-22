# How Data Flows
Explains how STL Metro datasets move from ingestion to Kafka, into PostgreSQL, and out through Flask REST APIs. Docker coordinates the stack.

## Data Sources (Excel / PDF / JSON)
Excel via excel_processor.py  
JSON via json_processor.py  
Web JSON via web_processor.py  
PDF via pdf_processor.py (exists but not completed)  
Input goal: accept raw files or payloads, normalize their structure, and publish to Kafka.

## Kafka Topics (ingestion + processing)
Exact topic strings present in code:  
Excel processed: excel-processed-topic   
JSON processed: JSON-data  
Web processed: web-processed-topic  
PDF processed: pdf-processed-topic

## PostgreSQL (write and read separation)
read_service DB connection (from read_service/app.py) uses environment variables:  
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD  
Engine URL: postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}
 
Conclusion: CQRS is logical only right now. Both write and read paths target the same physical PostgreSQL instance.  
If a true split is added later, separate URLs (e.g., WRITE_DATABASE_URL and READ_DATABASE_URL) should be used.

Schema bootstrapping (config/pg_init.sql):  
Creates test_table, snapshots, and historic_data.  
Tables like routes and events are referenced in the Flask code but not created here.  

## Flask REST APIs (query exposure)
From read_service/app.py:  
GET /health — health check for PostgreSQL connectivity  
GET /events/<event_type> — queries the events table by type and returns the 10 most recent rows  
GET /data/<data_type> — currently handles data_type == 'routes' by returning all routes  
GET /swagger and GET /swagger.json — serve OpenAPI documentation  

If the referenced tables (routes, events) do not exist, these endpoints will return errors until migrations are applied.

## Docker-based orchestration
One Postgres container defined in docker-compose.yml  
Kafka, Zookeeper, write_service, and read_service are expected  
.env uses 127.0.0.1:5432, meaning Flask apps connect to Postgres through a host-mapped port  
If the Flask services run inside Docker, PG_HOST must be set to the Docker service name (e.g., postgres) and PG_PORT to 5432  

For now, the system runs with local Flask services pointed at host-mapped Postgres.

## Text Flow Diagram
```text
[Data Sources]
  Excel      JSON/Web          PDF
     |           |              |
     v           v              v
[Ingestion + Processing Services]
  excel_processor     json_processor     pdf_processor
     |                     |                  |
     v                     v                  v
                  [Kafka Topics]
  "excel-porcessed-topic"   "JSON-data"   "web-processed-topic"   "pdf-processed-topic"
                          |
                          v
                 [Write Path / Consumer]
          validate → transform → upsert into Postgres
                          |
                          v
                      [PostgreSQL]
                 single container (stl_data)
                          |
                          v
                 [read_service Flask API]
      /health  /events/<type>  /data/<data_type>  /swagger
                          |
                          v
                        Clients
```

## Pseudocode Sketch
```python
# producers
def ingest_excel(file):
    for row in normalize(parse_excel(file)):
        producer.send("excel-porcessed-topic", serialize(row))

def ingest_json(payload):
    producer.send("JSON-data", serialize(normalize(payload)))

def ingest_web(payload):
    producer.send("web-processed-topic", serialize(normalize(payload)))

def ingest_pdf(payload):
    producer.send("pdf-processed-topic", serialize(normalize(payload)))

# consumer / write path
def on_kafka_record(topic, record):
    data = deserialize(record.value)
    upsert_postgres(route_table(topic), data)  # e.g., routes/events/etc.

# read-side
@app.get("/data/<data_type>")
def get_processed_data(data_type):
    table = map_type_to_table(data_type)  # e.g., routes
    rows = query_postgres(table)          # index-backed scans
    return jsonify({"data_type": data_type, "data": rows})