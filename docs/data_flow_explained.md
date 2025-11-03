Description:
Create a Markdown file (data_flow_explained.md) that explains how raw STL Metro datasets move through the system from ingestion to storage and querying.

Include the following sections:

Data Source (Excel/PDF/JSON formats)
Kafka Topics (ingestion + processing)
PostgreSQL (write and read separation)
Flask REST APIs (query exposure)
Docker-based orchestration
You may include a small text-based flowchart or pseudocode block to make it easier for non-coders to visualize.


## Data Lifecycle 

**Data Source** 
The STL Metro Data API can collect data from a variety of sources, such as
