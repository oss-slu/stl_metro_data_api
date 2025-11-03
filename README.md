# STL Metro Data API

**Overview**

The STL Data API project is a centralized, user-friendly platform designed to serve as a proxy for accessing and interacting with public data from various regional and municipal sources, with a focus on the St. Louis region. The project addresses challenges such as inconsistent data formats, lack of standardization, and repetitive efforts in compiling datasets by providing a RESTful API and a foundation for a future web portal. It is built using a CQRS (Command Query Responsibility Segregation) architecture with microservices, leveraging modern technologies for scalability and maintainability.

**Project Flow**

1. Data Ingestion: Fetch raw data (Excel, PDF, JSON, or web content) from public St. Louis data sources.
2. Raw Data Processing: Clean and transform raw data in memory, then send to Kafka for queuing.
3. Data Storage: Consume processed data from Kafka, store in PostgreSQL (snapshots, historic puts, aggregations), and delete raw data from memory.
4. Event Processing: Optimize short-term reads via event processors in the query-side microservice.
5. API Access: Expose RESTful endpoints (via Flask) for querying data, with Open API documentation.
6. Future Features: Add user subscriptions, web portal, and advanced optimizations.

**Tech Stack**

- Python 3.10+: Core language for data processing and API development.
- Flask Restful: Framework for building RESTful APIs in CQRS microservices.
- Kafka: Message broker for scalable, write-optimized data queuing (containerized).
- PostgreSQL: Database for storing processed data (containerized).
- Docker: Containerization for Kafka, PostgreSQL, and microservices.
- Open API (Swagger): API documentation for endpoints.
- SQLAlchemy: ORM for PostgreSQL interactions.

**Getting Started**

**Prerequisites**

- Python 3.10+: Install via python.org or pyenv.
- Docker Desktop: Install from docker.com (includes Docker Compose).
- psql Client: For PostgreSQL interaction (e.g., brew install postgresql on Mac).
- Git: For cloning the repository.
- VS Code: Recommended IDE with extensions (Python, Docker).

### Setup Instructions

Detailed setup is in [setup.md](./setup.md). Summary:

1. Clone the repo: `git clone https://github.com/oss-slu/stl_metro_dat_api && cd stl_metro_dat_api`.
2. Create and activate a virtual environment: `python -m venv venv && source venv/bin/activate` (Windows: `venv\Scripts\activate`).
3. Install dependencies: `pip install -r requirements.txt`.
4. Copy `.env.example` to `.env`: `cp .env.example .env` and update variables (e.g., `PG_PASSWORD`).
5. Register a new server in PostgreSQL pgAdmin 4 with port number `5433`
6. Start Kafka and PostgreSQL: `docker-compose -f docker/docker-compose.yml up -d`.
7. Verify setup: Run `python tests/basic_test.py` to confirm Kafka/PG connectivity.

### Project Structure

```
stl_metro_dat_api/
├── src/                  # Python source code
│   ├── write_service/    # CQRS Command side (data ingestion/processing)
│   └── read_service/     # CQRS Query side (event processors/API)
├── docker/               # Dockerfiles and Docker Compose configs
├── config/               # Kafka/PostgreSQL configurations
├── tests/                # Unit and integration tests
├── docs/                 # Open API (Swagger) specifications
├── requirements.txt      # Python dependencies
├── .env.example          # Template for environment variables
├── setup.md              # Detailed setup guide
└── README.md             # This file
```

## Running the Project

1. Start containers: `docker-compose --env-file.env -f docker/docker-compose.yml up -d`.
   - The write-service app should start automatically with Docker. To run the write-side app without Docker, go to the project's root directory in your terminal, and run `python -m src.write_service.app`.
2. Run read-side microservice: `cd src/read_service && python app.py`.
3. You can view the write-service app by going to `http://localhost:5000/` in your web browser.
4. View Open API docs: Access Swagger UI at `http://localhost:5001/swagger`.

**Important!** If you make changes to your code, you must update your Docker Containers so Docker can get the newest version of your code. To do this, run: `docker-compose -f docker/docker-compose.yml build`

## Getting Started with Contributions

Thank you so much for wanting to contribute to our project! Please see the [Contribution Guide](/docs/CONTRIBUTOR_JOURNEY_MAP.md) on how you can help.

## Testing

- Run unit tests: `pytest tests/`.
- Check connectivity: `python tests/basic_test.py`.

## Contact

For questions, reach out to the Tech Lead via Slack or GitHub Issues. Report bugs or suggest features in the Issues tab.
