# Cross-Team Guide: Understanding CQRS + Kafka Architecture in the STL Metro Data API  
Version: 1.0  
Authors: STL Metro Data API Tech Lead  
Last Updated: December 2025  

## 1. Overview
The STL Metro Data API uses a CQRS (Command Query Responsibility Segregation) architecture backed by Apache Kafka to ingest, process, persist, and expose regional public datasets. This guide provides a cross-team reference to help other Open Source with SLU teams understand the patterns, workflows, and reusable practices behind our system.

This is a high-level, reusable explanation intended for teams exploring distributed systems, event-driven pipelines, microservices, or open data infrastructures.

---

## 2. Why CQRS?
CQRS separates **write-side responsibilities** (commands + ingestion) from **read-side responsibilities** (queries + public API responses). In the STL Metro Data API:

- **Write Service** ingests external datasets and emits domain events.  
- **Read Service** consumes processed events and updates a query-optimized store.  
- **Kafka** acts as the event backbone enabling loose coupling and replayability.

### Benefits
- Independent scaling of read/write concerns  
- Event-driven history and reproducibility  
- Clear system boundaries and maintainability  
- Supports future integrations (analytics, pipelines, dashboards)

---

## 3. Why Kafka?
Kafka backs the project as the central event log for all dataset updates. It enables:

- Asynchronous ingestion  
- Replayable pipelines  
- Loose coupling between services  
- Robust processing even in failure scenarios  

Kafka topics reflect dataset types (e.g., transit routes, census segments, public assets). Any consumer—internal or future teams—can subscribe independently.

---

## 4. System Architecture At a Glance
1. **Ingestion / Write Service**  
   - Fetches datasets  
   - Normalizes input  
   - Validates records  
   - Emits events to Kafka (e.g., `dataset_name.record_added`)

2. **Kafka Event Bus**  
   - Stores ingestion events  
   - Guarantees ordering & durability  
   - Enables downstream processing

3. **Read Service**  
   - Subscribes to Kafka events  
   - Transforms and stores query models in PostgreSQL  
   - Exposes REST API endpoints with clean read models

4. **Client Consumers**  
   - Query regional data (no authentication required in local development)  
   - Visual analytic tools, future dashboards, and external partners can integrate

For a deeper technical view, see:
- [CQRS and Event Sourcing](/docs/cqrs_event_sourcing_summary.md)
- [Data Flow](/docs/data_flow_explained.md)

---

## 5. Event Flow Example
1. Dataset is ingested by Write Service  
2. A validated record triggers an event:  
   `stl_metro.<dataset>.record_added`  
3. Kafka stores the event and makes it available for consumers  
4. Read Service consumes the event  
5. Read Service updates the PostgreSQL read database  
6. API endpoint reflects the new state

Event-driven flow ensures correctness, consistency, and future extensibility.

---

## 6. Reusable Patterns for Other OSS SLU Teams

### 6.1 Clear contract between read/write services  
Define explicit message schemas so services evolve independently.

### 6.2 Event-driven over REST chaining  
Avoid service-to-service calls. Kafka eliminates cascading failures.

### 6.3 Idempotent consumers  
Ensure repeated messages do not corrupt data.

### 6.4 Replayable state  
Kafka allows rebuilding the read database from scratch using past events.

### 6.5 Documented schema versions  
Track schema evolution using JSON schemas shared in the repo.

---

## 7. Lessons Learned
- Small, consistent message shapes reduce downstream ambiguity  
- Early validation in the write layer prevents propagation of bad records  
- Event replay is essential for debugging and development  
- Using PostgreSQL as a read model improves query latency  
- Threat modeling (see [Threat MOdel](/docs/threat_model.md)) clarifies future deployment needs  

---

## 8. How Other Teams Can Use This Guide
This documentation is intended for:
- Teams exploring distributed or microservice designs  
- Projects using streaming workflows  
- Student engineers learning event-driven systems  
- OSS SLU teams looking to scale architectures  

Teams may adapt the patterns when:
- Ingesting large datasets  
- Requiring replayable processing  
- Designing loosely coupled services  

---

## 9. References  
- [CQRS and Event Sourcing](/docs/cqrs_event_sourcing_summary.md)
- [Data Flow](/docs/data_flow_explained.md)
- [Threat MOdel](/docs/threat_model.md)
- [STL Metro Data API GitHub Repository](https://github.com/oss-slu/stl_metro_data_api)  
