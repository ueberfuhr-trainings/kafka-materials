---
title: Schulungsunterlagen
---

# Event-basierte Kommunikation mit Kafka

Willkommen bei den Übungen zur Schulung "Event-basierte Kommunikation mit Kafka".

## Vorlagen

Die Vorlagen für die Übungen findest Du hier:

- [Quarkus](https://github.com/ueberfuhr-trainings/quarkus-kafka)
- [Spring Boot](https://github.com/ueberfuhr-trainings/spring-boot-kafka)

Klone oder lade das Repository in der Variante Deiner Wahl herunter (Branch `main`).

## Die Projekte

Jedes Repository enthält zwei Microservices:

| Service                         | Beschreibung                                                |
|---------------------------------|-------------------------------------------------------------|
| **account-service-provider**    | Verwaltet Kunden (CRUD) und publiziert Events an Kafka      |
| **statistics-service-provider** | Konsumiert Kunden-Events und pflegt Statistiken über Kunden |

Die Services nutzen eine Schichtenarchitektur (angelehnt an Onion Architecture) mit den Schichten
**boundary** (REST-API), **domain** (Geschäftslogik), **persistence** (Datenbank) und **kafka**.
Zentrale Klasse ist der `CustomersService` im Domain-Layer. Über Interceptors werden bei
CRUD-Operationen automatisch Events gefeuert (CDI Events bzw. Spring Events), die von der
Kafka-Schicht weiterverarbeitet werden.

## Kafka-Konzepte und -Ökosystem

Eine ausführliche Erläuterung der Kafka-Konzepte findest Du in
diesem [Baeldung-Artikel](https://www.baeldung.com/apache-kafka).

Weitere Themen:

* [Konfiguration (Properties)](docs/properties.html)
* [Fehlerbehandlung](docs/exception-handling.html)
* [JSON Schema & Schema Registry](docs/json-schema.html)
* [CloudEvents](docs/cloud-events.html)
* [Apache AVRO](docs/avro.html)
* [Testen](docs/testing.html)

## Übungen

* [Story 1: Kafka-Producer](issues/producer.html) — Kundenänderungen als Events versenden
* [Story 2: Kafka-Consumer](issues/consumer.html) — Kundenstatistiken aus Events ableiten

## Wiederholung

* [Quiz zu Kafka-Konzepten](quiz/concepts.html)