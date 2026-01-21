# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How to Work in This Repository

- This is a multi-service Java monorepo.
- Each service is an independent Spring Boot application with its own `pom.xml`.
- Kafka is the ONLY mechanism for inter-service communication.
- Do NOT introduce synchronous REST calls between services.
- Prefer correctness, clarity, and production-style patterns over feature count.
- Make minimal changes per step and wait for confirmation before proceeding.

## Project Goal

Build a production-style, event-driven microservices platform using Spring Boot and Apache Kafka that demonstrates real-world backend engineering practices. The system should consist of multiple independently deployable services that communicate asynchronously via Kafka events (no direct service-to-service REST calls), implementing a complete order workflow with inventory, billing, and notification services.

The project should emphasize:

 - Event-driven architecture with well-defined event schemas and topic versioning

 - Reliability and correctness through idempotent consumers, retries, and dead-letter queues

 - Transactional safety using patterns such as the outbox pattern where appropriate

 - Observability via structured logging, metrics, and distributed tracing

Testability with integration tests (e.g., Kafka + Postgres via Testcontainers)

The goal is not feature breadth, but to clearly demonstrate scalable system design, fault tolerance, and backend engineering maturity suitable for software engineering internships. If you ever unsure of how something should be implemented you should ask to receive feedback from the User.

## Repository Structure

This project is a multi-service monorepo.

Each microservice is an independent Spring Boot application with its own
`pom.xml` and `src/main/java` structure, located under:

- services/order-service
- services/inventory-service
- services/billing-service
- services/notification-service

Kafka is used for all inter-service communication via Spring for Apache Kafka.

## Build Commands

```bash
# Build all modules
mvn clean install

# Build a specific service
mvn clean install -pl services/order-service

# Run tests
mvn test

# Run tests for a specific service
mvn test -pl services/inventory-service

# Run a single test class
mvn test -pl services/order-service -Dtest=OrderServiceTest

# Run a specific service
mvn spring-boot:run -pl services/order-service
```

## Service Ports

- order-service: 8081
- inventory-service: 8082
- billing-service: 8083
- notification-service: 8084

## Infrastructure Requirements

- PostgreSQL on localhost:5432 (databases: orders, inventory, billing, notifications)
- Apache Kafka on localhost:9092
