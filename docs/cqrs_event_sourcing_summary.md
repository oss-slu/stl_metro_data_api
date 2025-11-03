# CQRS and Event Sourcing in STL Metro Data API

### What is CQRS and Why it Matters

**CQRS(Command Query Responsibility Segregation)** separates write operations(commands) from read oeprations(queries).
Why it matters:

- Read and write operations have different performance needs
- Allows independent scaling of reads and writes
- Simplifies system design

### Command Side vs Query Side

Command Side (Write Model)

- Changes the system's state
- In our project: fetch data, clean data, publish to Kafka
  Query Side (Read Model)
- Retrieves data for users
- In our project: read from Kafka, store in PostgreSQL, serve via API

### What is Event Sourcing

**Event Sourcing** stores every change as a sequence of events instead of just the current state.

Example: Banking

```
Event 1: Deposit $100
Event 2: Withdraw $20
Event 3: Deposit $50
Current Balance = $130
```

### Benefits

**Scalability**

- Scale API servers independently from data scapers
- Handle millions of API requests without slowing down data collection
  **Maintainability**
- Clear separation between data collection and data serving
- Teams can work indpendently
- Easier to test each part
  **Reliability**
- Kafka stores complete history
- Can rebuild database by replaying events
- No data loss during failures
