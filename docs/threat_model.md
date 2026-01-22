# STL Metro Data API – Threat Model

**Version:** 1.0  
**Date:** December 2025  
**Status:** Initial Assessment (Local Environment Only)  
**Authors:** STL Metro Data API Team (Tech Lead + Developers)

---

## 1. Executive Summary

### 1.1  
The STL Metro Data API is an open source CQRS-based microservices system developed to ingest, process, and expose public regional datasets from the St. Louis area. The system is currently used exclusively in local development environments, with no authentication on the `write_service` or `read_service`, and handles only public/non-sensitive data (no PII).

### 1.2  
The primary security objectives for the project at this stage are to maintain:

- Integrity of processed datasets  
- Stability and correctness of the ingestion pipeline  
- Safe open-source contribution practices  
- Awareness of deployment security risks for future teams  

### 1.3  
While the current environment limits exposure to external threats, this threat model identifies application, infrastructure, and open-source process risks that would become significant if the system evolves toward shared or production-like deployments.

---

## 2. Product Context

### 2.1 System Purpose

#### 2.1.1  
The STL Metro Data API provides a unified, developer-friendly platform for interacting with publicly available St. Louis regional data.

#### 2.1.2  
The system implements CQRS (Command Query Responsibility Segregation):

- **Command Side (`write_service`)**
  - Ingests public datasets in various formats (Excel, PDF, JSON, web content)
  - Cleans and transforms raw data
  - Publishes processed messages to Kafka

- **Query Side (`read_service`)**
  - Reads data from PostgreSQL
  - Exposes RESTful API endpoints with Swagger documentation

#### 2.1.3  
The system does not collect, process, or store any sensitive personal information.

---

### 2.2 Architecture Overview

#### 2.2.1  
The system architecture consists of the following components:

- Zookeeper (Confluent)  
- Kafka Broker (Confluent)  
- PostgreSQL 14 (`stl_data` database)  
- `write_service` (Flask microservice, port `5005`)  
- `read_service` (Flask microservice, port `5001`)  
- `pdf_consumer` (Kafka consumer -> PostgreSQL writer)  
- Optional `frontend` served locally at port `9000`  

#### 2.2.2  
All services run within a local Docker Compose environment. Ports for Kafka and PostgreSQL are mapped to the local host.

#### 2.2.3  
GitHub Actions workflows support:

- CI (linting and unit tests)  
- Docker Validation (image builds and smoke tests)  
- Integration Tests (full-stack `docker-compose` test)  

---

### 2.3 Data Characteristics

#### 2.3.1  
The system ingests only public regional datasets.

#### 2.3.2  
Data types include:

- Excel spreadsheets  
- PDF documents  
- JSON APIs  
- Web content from municipal or regional sources  

#### 2.3.3  
Primary security focus: **data integrity and correctness**, rather than confidentiality.

---

### 2.4 Users and Actors

#### 2.4.1  
The following actors interact with the system:

- Internal development team  
- Open-source contributors via GitHub  
- Local API consumers (scripts, tools, frontend)  
- CI/CD automation (GitHub Actions)  

#### 2.4.2  
No external end users or authenticated roles exist at this stage.

---

### 2.5 Deployment Context

#### 2.5.1  
The system is currently used only in local environments.

#### 2.5.2  
No components are publicly exposed or deployed to a cloud or server environment.

#### 2.5.3  
APIs (`write_service` and `read_service`) have no authentication by design during this phase.

---

## 3. Attack Surface Analysis

### 3.1 Network Entry Points

#### 3.1.1  
Active service endpoints include:

| Component      | Entry Type | Endpoint                                                |
|----------------|-----------|---------------------------------------------------------|
| `write_service` | HTTP      | `http://localhost:5005/`                                |
| `read_service`  | HTTP      | `http://localhost:5001/` (Swagger: `http://localhost:5001/swagger`) |
| `frontend`      | HTTP      | `http://localhost:9000/`                                |
| `kafka`         | TCP       | `localhost:9092`                                        |
| `postgres`      | TCP       | `localhost:${PG_PORT}` -> container `5432`             |

#### 3.1.2  
These entry points are accessible only within the local machine unless the user explicitly exposes them.

---

### 3.2 Data Flow Overview

#### 3.2.1 Ingestion Phase

- `write_service` fetches external public data  
- Processes data in memory  
- Publishes structured messages to Kafka  

#### 3.2.2 Processing Phase

- `pdf_consumer` subscribes to Kafka topics  
- Validates and transforms messages  
- Writes processed data to PostgreSQL (`stl_data`)  

#### 3.2.3 Query Phase

- `read_service` receives API requests  
- Performs database queries  
- Returns processed dataset responses  

#### 3.2.4 Frontend Consumption

- Static frontend retrieves data from `read_service`  

---

### 3.3 Trust Boundaries

#### 3.3.1  
Key trust boundaries include:

- External public data sources -> `write_service`  
- Local client/frontend -> `read_service`  
- `write_service` -> Kafka broker  
- GitHub contributors -> repository and CI workflows  

#### 3.3.2  
Crossing these boundaries introduces potential areas for threat activity.

---

## 4. Threat Catalog

### 4.1 Application Layer Threats

#### A1. SQL Injection via Improper Query Handling

- **Category:** Tampering, Information Disclosure  
- **Cause:** Unvalidated user inputs may be concatenated into SQL statements  
- **Impact:** Data corruption, unauthorized data access, unexpected behavior  
- **Likelihood:** Requires assessment of current code patterns  
- **Relevance:** Local now, critical if future APIs are exposed  

#### A2. Local API Abuse Leading to Resource Exhaustion

- **Category:** Denial of Service  
- **Cause:** Expensive or repeated heavy queries  
- **Impact:** API slowdown or failure during development  
- **Likelihood:** Medium in current workflows  

---

### 4.2 Kafka Pipeline Threats

#### K1. Malformed Kafka Messages Causing Consumer Failures

- **Category:** Tampering (accidental or malicious), DoS  
- **Cause:** Schema drift, ingestion bugs, invalid message formatting  
- **Impact:** Consumer crashes, partial writes, data inconsistencies  
- **Likelihood:** Medium  

#### K2. Kafka Port Reuse in Future Non-Local Deployments

- **Category:** Tampering, Denial of Service  
- **Cause:** Kafka port (`9092`) mapped to host may be exposed on future server deployments  
- **Impact:** Topic flooding, data corruption  
- **Likelihood:** Low in current context, high if mis-deployed  

---

### 4.3 Database Threats

#### D1. Weak or Mishandled PostgreSQL Credentials

- **Category:** Spoofing, Tampering  
- **Cause:** Weak `${PG_PASSWORD}`, accidental `.env` commits  
- **Impact:** Full DB compromise (if externally reachable)  
- **Likelihood:** Medium  

#### D2. Database Exposure in Future Deployments

- **Category:** Information Disclosure, Tampering  
- **Cause:** Host-mapped `${PG_PORT}` left open in a remote deployment  
- **Impact:** Unauthorized DB access  
- **Likelihood:** Low now, high if future deployments reuse same config  

---

### 4.4 CI/CD and Open-Source Threats

#### C1. Malicious Pull Requests

- **Category:** Tampering, Elevation of Privilege  
- **Cause:** PRs altering ingestion logic, CI workflows, or dependencies  
- **Impact:** Codebase compromise, data corruption  
- **Likelihood:** Low–Medium  

#### C2. Misconfigured GitHub Actions Workflows

- **Category:** Elevation of Privilege  
- **Cause:** Running untrusted code with excessive permissions  
- **Impact:** Repository manipulation, artifact tampering  
- **Likelihood:** Medium  

#### C3. Dependency or Base Image Supply-Chain Attacks

- **Category:** Tampering  
- **Cause:** Compromised Python libraries or Docker base images  
- **Impact:** Arbitrary code execution, data integrity issues  
- **Likelihood:** Moderate  

---

### 4.5 Local Environment Misuse

#### L1. Reuse of Development Docker Compose on Public Servers

- **Category:** Tampering, Denial of Service  
- **Cause:** Future teams or users deploying the same compose file without hardening  
- **Impact:** Exposed ports, unauthenticated APIs, remote misuse  
- **Likelihood:** Unknown, mitigated through documentation  

---

## 5. Risk Prioritization

### 5.1 Summary Matrix

| Threat                                  | Impact | Likelihood | Priority (Current Context) |
|-----------------------------------------|--------|-----------|----------------------------|
| A1 – SQL Injection                      | High   | Unknown   | High                       |
| K1 – Malformed Kafka Messages           | High   | Medium    | High                       |
| D1 – Credential Mismanagement           | High   | Medium    | High                       |
| C1 – Malicious PRs                      | High   | Low–Med   | Medium–High                |
| C3 – Dependency Attacks                 | High   | Medium    | Medium–High                |
| A2 – Local DoS                          | Medium | Medium    | Medium                     |
| C2 – Unsafe CI Workflows                | High   | Low–Med   | Medium                     |
| L1 – Reuse of Compose in Production     | High   | Unknown   | Medium                     |
| K2 – Kafka Exposure in Future Deploys   | High   | Low       | Medium                     |
| D2 – DB Exposure in Future Deploys      | High   | Low       | Medium                     |

---

## 6. Mitigation Strategy

### 6.1 Near-Term Recommendations

#### M1. Enforce Safe Database Querying  
**Addresses:** A1  

- Use parameterized queries or SQLAlchemy ORM exclusively  
- Avoid string concatenation for SQL formation  
- Add simple test cases to detect injection patterns  

---

#### M2. Add Kafka Message Validation  
**Addresses:** K1  

- Validate message schema fields in `pdf_consumer`  
- Log and skip invalid messages instead of crashing  

---

#### M3. Strengthen Credential Handling Practices  
**Addresses:** D1  

- Encourage strong passwords in `.env.example`  
- Keep `.env` files out of the repository  
- Add notes in README regarding environment variable security  

---

#### M4. Document Security Assumptions Explicitly  
**Addresses:** L1, K2, D2  

Add a **“Security & Deployment Notes”** section to README:

- Project intended for local development only  
- APIs are unauthenticated and not production-ready  
- Kafka and PostgreSQL should be firewalled in any remote environment  

---

#### M5. Strengthen Contribution and Review Policies  
**Addresses:** C1  

- Require thorough reviews for PRs touching dependencies, Dockerfiles, CI, or core ingestion logic  
- Add brief “Security Guidelines for Contributors” to documentation  

---

### 6.2 Medium to Long-Term Recommendations

#### M6. Introduce Optional API Authentication for Future Deployments

- Could include API keys or reverse-proxy enforcements  

#### M7. Provide Hardened Production Deployment Examples

- Example `docker-compose.prod.yml` without exposed ports  
- Network isolation between services  
- Optional TLS and authentication via a reverse proxy  

#### M8. Integrate Lightweight Security Tooling

- Add `bandit` to CI for static analysis  
- Enable Dependabot or similar tools for dependency monitoring  

---

## 7. Open Questions for Future Teams

### 7.1  
Will the project ever be deployed to public servers?

- If yes, authentication, TLS, and stricter network controls must be implemented.

### 7.2  
Will new features involve user accounts, subscriptions, or custom dashboards?

- If yes, this threat model must expand to include identity and access management.

### 7.3  
Will the API become a dependency for downstream municipal or academic projects?

- If yes, availability requirements increase and system hardening becomes more important.

### 7.4  
Will secrets ever be added to CI workflows?

- If yes, workflows must be reviewed for secure handling of secrets.

---

