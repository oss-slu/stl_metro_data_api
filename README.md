# STL Metro Data API
![CI](https://github.com/oss-slu/stl_metro_data_api/actions/workflows/ci.yml/badge.svg)
![Docker Validation](https://github.com/oss-slu/stl_metro_data_api/actions/workflows/docker-validation.yml/badge.svg)

**Overview**

The STL Data API project is a centralized, user-friendly platform designed to serve as an all-in-one place for accessing and interacting with public data about the St. Louis region. The goal is to make data about the City of St. Louis very easy to use and access for both developers and the general public. The project addresses challenges such as inconsistent data formats, lack of standardization, and repetitive efforts in compiling datasets by providing a RESTful API and a foundation for a future web portal. It is built using a CQRS (Command Query Responsibility Segregation) architecture with microservices, leveraging modern technologies for scalability and maintainability.

**Technical Overview**

- [How Data Flows](/docs/data_flow_explained.md)


**Project Flow**

1. Data Ingestion: Fetch raw data (Excel, PDF, JSON, or web content) from public St. Louis data sources.
2. Raw Data Processing: Clean and transform raw data in memory, then send to Kafka for queuing.
3. Data Storage: Consume processed data from Kafka, store in PostgreSQL (snapshots, historic puts, aggregations), and delete raw data from memory.
4. Event Processing: Optimize short-term reads via event processors in the query-side microservice.
5. API Access: Expose RESTful endpoints (via Flask) for querying data, with Open API documentation using Swagger.
6. Frontend U.I.: Create a frontend U.I. that will make it easy for developers and the general public to use this data.
7. Future Features: Add user subscriptions, web portal, and advanced optimizations.

Architecture overview diagram here: [Architecture](/docs/architecture_overview.png)

**Tech Stack**

- Python 3.10+: Core language for data processing and API development.
- Flask Restful: Framework for building RESTful APIs in CQRS microservices.
- Kafka: Message broker for scalable, write-optimized data queuing (containerized).
- PostgreSQL: Database for storing processed data (containerized).
- Docker: Containerization for Kafka, PostgreSQL, and microservices.
- Open API (Swagger): API documentation and U.I. for endpoints.
- SQLAlchemy: ORM for PostgreSQL interactions.

**Getting Started**

**Prerequisites**

- Python 3.10+: Install via python.org or pyenv.
- Docker Desktop: Install from docker.com (includes Docker Compose).
- pgAdmin 4: Recommended GUI
- Git: For cloning the repository.
- VS Code: Recommended IDE with extensions (Python, Docker).

### Setup Instructions

Detailed setup is in [setup.md](./setup.md). Summary:

1. Clone the repo: `git clone https://github.com/oss-slu/stl_metro_dat_api && cd stl_metro_dat_api`.
2. Create and activate a virtual environment: `python -m venv venv && source venv/bin/activate` (Windows: `venv\Scripts\activate`).
3. Install dependencies: `pip install -r requirements.txt`.
4. Copy `.env.example` to `.env`: `cp .env.example .env` and update variables (e.g., `PG_PASSWORD`).
5. Start Infrastructure: `docker-compose --env-file .env -f docker/docker-compose.yml up -d`.
6. Verify setup: Run `python tests/basic_test.py` to confirm services are healthy.

### Project Structure

```
stl_metro_dat_api/
├── src/                  # Python source code
│   ├── write_service/    # CQRS Command side (data ingestion/processing/consumers)
│   └── read_service/     # CQRS Query side (event processors/API/frontend/Swagger)
├── docker/               # Dockerfiles and Docker Compose configs
├── config/               # Kafka/PostgreSQL configurations
├── tests/                # Unit and integration tests
├── docs/                 # Project guides and documentation
├── requirements.txt      # Python dependencies
├── .env.example          # Template for environment variables
├── setup.md              # Detailed setup guide
└── README.md             # This file
```

## Running the Project

1. Start containers: `docker-compose --env-file .env -f docker/docker-compose.yml up -d`.
   - The write-service app should start automatically with Docker. To run the write-side app without Docker, go to the project's root directory in your terminal, and run `python -m src.write_service.app`.
   - The read-service app along with the front-end (excellence project) should also automatically start with Docker. To run the read-side app without Docker, go to the project's root directory in your terminal, and run `python -m src.read_service.app`.
3. You can view the write-service app by going to `http://localhost:5005/` in your web browser.
4. View Open API docs: Access Swagger UI at `http://localhost:5001/swagger`.
5. The front-end (excellence project) should automatically start with Docker.
   - To view the front-end, go to `http://localhost:5001/index.htm` in your web browser.

**Important!** If you make changes to your code, you must update your Docker Containers so Docker can get the newest version of your code. To do this, run: `docker-compose -f docker/docker-compose.yml build`

## How to run the JSON fetcher, processor, and consumer (how to insert ARPA funds into database)
Here is how you run the JSON fetcher, JSON processer, and JSON consumer.
This is also how ARPA (American Rescue Plan Act) data from the City of St. Louis Open Data Portal
is saved into the database:
1. Start up the project's Docker containers.
2. Do one of the following:
   - Go to http://localhost:5005/json. The ARPA data will be saved into the database.
   You should see a webpage displaying what was saved 
   in the database along with the Kafka status. The PostgreSQL 
   application, if connected properly to the project, should also display the table data.

   - OR run `python -m src.write_service.consumers.json_consumer` from the project's root folder. 
   The ARPA data will be saved into the database. The terminal should display what was 
   received from Kafka and what was inserted into the database. The PostgreSQL application, 
   if connected properly to the project, should also display the table data.

Once ARPA data is in the database, you can see the data in three ways:
1. Go to http://localhost:5001/api/arpa to see the ARPA endpoint.
2. Go to http://localhost:5001/swagger to see the Swagger U.I..
3. Go to http://localhost:5001/arpa.htm to see the ARPA frontend table U.I. (excellence project)

## Getting Started with Contributions

Thank you so much for wanting to contribute to our project! Please see the [Contribution Guide](/docs/CONTRIBUTING.md) on how you can help.

## Testing

- Run unit tests: `pytest tests/`.
- Check connectivity: `python tests/basic_test.py`.

## Technical Overview

The STL Metro Data API uses CQRS architecture to separate data collection (write operations) from data serving (read operations).

**Key Architectural Patterns:**

- [CQRS and Event Sourcing](/docs/cqrs_event_sourcing_summary.md) - Explains how we use Kafka to separate write and read operations

**System Components:**

- Write Service: Data ingestion and processing
- Kafka: Event streaming and message queue
- Read Service: PostgreSQL database and Flask API

## Security & Threat Model

The STL Metro Data API includes a detailed **Threat Model** that documents the system’s security assumptions, identified risks, and recommended mitigations.  
This resource helps contributors, maintainers, and future teams understand the project’s security posture and how it may evolve.

**View the full Threat Model:**  
**[docs/threat_model.md](docs/threat_model.md)**

**Key Notes:**

- The project is designed for **local development environments only**.  
- API services currently include **no authentication**.  
- Kafka and PostgreSQL should **not** be exposed publicly without additional hardening.  
- Contributors making architectural or security-related changes should review the Threat Model before submitting PRs.

For questions regarding security considerations, please reach out to the project’s Tech Lead.


## Code of Conduct

We are committed to fostering an inclusive and respectful community for all contributors to the STL Data API project. Please review our [Code of Conduct](docs/CODE_OF_CONDUCT.md) for guidelines on expected behavior, reporting violations, and our pledge to a harassment-free environment. By participating, you agree to uphold these standards.

## Contact

For questions, reach out to the Tech Lead via Slack or GitHub Issues. Report bugs or suggest features in the Issues tab.
