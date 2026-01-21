# Setup Guide for STL Data API

This guide provides step-by-step instructions to set up the development environment for the STL Data API project. It is designed for junior developers with basic Python knowledge. Follow these steps to clone the repository, install dependencies, and configure the environment to start contributing.

## Prerequisites

Before starting, ensure you have the following installed on your system:

- **Python 3.13+**: Download from [python.org](https://www.python.org/downloads/) or use `pyenv` for version management.
  - Verify: `python --version` (should output 3.13 or higher).
- **Docker Desktop**: Install from [docker.com](https://www.docker.com/products/docker-desktop/) (includes Docker Compose).
  - Verify: `docker --version` and `docker-compose --version`.
- **pgAdmin 4**: Recommended GUI for viewing data. Install via `brew install --cask pgadmin4` (Mac) or from [pgadmin.org](https://www.pgadmin.org/).
- **Git**: For repository cloning.
  - Verify: `git --version`.
- **VS Code**: Recommended IDE with extensions:
  - Python (by Microsoft)
  - Docker (by Microsoft)
  - GitLens (optional for Git integration)
  - Install: [code.visualstudio.com](https://code.visualstudio.com/).

## Step-by-Step Setup

### 1. Clone the Repository

Clone the project repository to your local machine and navigate to the project directory.

```bash
git clone https://github.com/oss-slu/stl_metro_dat_api
cd stl_metro_dat_api
```

- Ensure you have write access to the repository (contact the Tech Lead if issues arise).
- The repository contains the initial structure, including `src/`, `docker/`, `tests/`, and more (see README.md).

### 2. Set Up Python Virtual Environment

Create and activate a virtual environment to isolate project dependencies.

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

- Verify activation: Your terminal prompt should show `(venv)`.
- To deactivate later: Run `deactivate`.

### 3. Install Python Dependencies

Install the required Python libraries listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```

- Key dependencies include:
  - `flask` and `flask-restful`: For API microservices.
  - `kafka-python`: For Kafka integration.
  - `psycopg2-binary`: For PostgreSQL connectivity.
  - `pandas`, `requests`, `beautifulsoup4`, `PyPDF2`, `openpyxl`: For data ingestion/processing.
  - `sqlalchemy`: For PostgreSQL ORM.
  - `flask-swagger-ui`: For Open API documentation.
  - `pytest`: For testing.
- Verify: `pip list` should show installed packages (e.g., `flask==3.0.0`).

### 4. Configure Environment Variables

Copy the provided `.env.example` file to `.env` and update it with local settings.

```bash
cp .env.example .env
```

Edit `.env` with a text editor (e.g., VS Code). Example content:

```env
KAFKA_BROKER=localhost:9092
PG_HOST=localhost
PG_PORT=5432
PG_DB=stl_data
PG_USER=postgres
PG_PASSWORD=123456
PYTHONPATH=src

# Kafka Configuration
KAFKA_BROKER_ID=1
KAFKA_BROKER_HOST=kafka
KAFKA_BROKER_PORT=9092
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
ZOOKEEPER_HOST=zookeeper
ZOOKEEPER_PORT=2181
KAFKA_TOPICS=raw-data-topic,processed-data-topic
RAW_DATA_TOPIC=raw-data-topic
PROCESSED_DATA_TOPIC=processed-data-topic

```

- **Note**: Use a secure `PG_PASSWORD` for local development. Do not commit `.env` to Git (it’s ignored via `.gitignore`).
- These variables configure connections to Kafka and PostgreSQL containers.

### 5. Start Docker Containers

Use Docker Compose to spin up Kafka (with Zookeeper) and PostgreSQL containers.

```bash
docker-compose --env-file .env -f docker/docker-compose.yml up -d
```

- Verify containers are running: `docker ps` (should list `zookeeper`, `kafka`, and `postgres`).
- **Important!** If you make changes to your code, you must update your Docker Containers so Docker can get the newest version of your code. To do this, run: `docker-compose --env-file .env -f docker/docker-compose.yml build`
- To stop: `docker-compose -f docker/docker-compose.yml down`.
  - If not initialized correctly, remove volumes when taking down containers: `docker-compose --env-file .env -f docker/docker-compose.yml down -v` or `docker-compose --env-file .env -f docker/docker-compose.yml up -d --build`
- If issues occur (e.g., port conflicts), check logs: `docker logs <container_name>`.

### 6. Verify Database in pgAdmin 4

Register your Docker database to see the data:

- Host name/address: localhost
- Port: 5432
- Maintenance database: stl_data
- Username: postgres
- Password: 123456

### 7. Verify Connectivity

Run the connectivity test script to ensure Kafka and PostgreSQL are accessible.

```bash
python tests/basic_test.py
```

- This script tests:
  - Producing/consuming a sample message to Kafka.
  - Connecting to PostgreSQL and executing a sample query.
- If errors occur, check `.env` settings, ensure Docker containers are running, or consult the Tech Lead.

### 8. Run a Sample Microservice

Test the Flask skeleton for the write-side microservice.

- The write-service app should start automatically with Docker. To run the write-side app without Docker, go to the project's root directory in your terminal, and run `python -m src.write_service.app`.
- Open a browser and go to http://localhost:5005/. A webpage should appear.

Test the Flask skeleton for the read-side microservice.

- The read-service app along with the front-end (excellence project) should also automatically start with Docker. To run the read-side app without Docker, go to the project's root directory in your terminal, and run `python -m src.read_service.app`.
- Open a browser and go to http://localhost:5001/.
- Or use `curl`: `curl http://localhost:5001/swagger`. Expected output: Swagger opened in browser.

### 9. Run Tests

Execute the test suite to ensure the environment is correctly set up.

```bash
pytest tests/
```

- This runs unit tests in the `tests/` directory.
- Expected: All tests pass (initially, only connectivity tests exist).
- If tests fail, check error messages and ensure Docker services are up.

### 10. Secondary Front-end (Excellence Project)
The front-end (excellence project) should automatically start with Docker.
   - To view the front-end, go to http://localhost:5001/index.htm in your web browser.

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

## Development Workflow

- **Branching**: Create feature branches from `develop` (e.g., `git checkout develop && git checkout -b feature/sprint1-dev1-kafka-setup`).
- **Coding**: Write code in `src/`, tests in `tests/`, and configs in `config/` or `docker/`. Include docstrings and tests in your code.
- **Commits**: Use clear messages (e.g., `git commit -m "Add Kafka consumer for web data"`).
- **Pull Requests**: Push your branch (`git push origin feature/your-branch`) and create a PR to `develop`. Assign a reviewer.
- **Kanban Board**: Check GitHub Projects for assigned issues and sprint tasks.


## CI / CD Overview

- **CI** (`.github/workflows/ci.yml`): runs linting (flake8) and tests (`pytest`) on pushes to `main/develop` and PRs targeting `main`. It uses `actions/cache` to cache pip downloads based on requirements.txt.
- **Docker Validation** (`.github/workflows/docker-validation.yml`): builds `write_service` and `read_service` images with dynamic tags and runs a lightweight smoke test. No images are pushed to any registry.
- To run locally: run `pytest` for tests; use `docker build -f docker/write_service/Dockerfile -t local/write_service:TAG .` to locally validate the Dockerfile.
