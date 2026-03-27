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

| Service                        | Beschreibung                                                  |
|--------------------------------|---------------------------------------------------------------|
| **account-service-provider**   | Verwaltet Kunden (CRUD) und publiziert Events an Kafka        |
| **statistics-service-provider**| Konsumiert Kunden-Events und pflegt Statistiken über Kunden   |

Die Services nutzen eine Schichtenarchitektur (angelehnt an Onion Architecture) mit den Schichten
**boundary** (REST-API), **domain** (Geschäftslogik), **persistence** (Datenbank) und **kafka**.
Zentrale Klasse ist der `CustomersService` im Domain-Layer. Über Interceptors werden bei
CRUD-Operationen automatisch Events gefeuert (CDI Events bzw. Spring Events), die von der
Kafka-Schicht weiterverarbeitet werden.

## Stories

In den folgenden Übungen setzt Du zwei User Stories um:

### Story 1: Kafka-Producer (account-service-provider)

> _Wenn ein Kunde angelegt, geändert oder gelöscht wird, soll eine Benachrichtigung per Kafka verschickt werden._

Du implementierst im **account-service-provider** einen Kafka-Producer, der die Domain-Events
(`CustomerCreatedEvent`, `CustomerReplacedEvent`, `CustomerDeletedEvent`) konsumiert und als
Nachrichten an ein Kafka-Topic sendet.

**Musterlösung:** Branch [`feature/producer`](https://github.com/ueberfuhr-trainings/quarkus-kafka/compare/main...feature/producer)
([Spring Boot](https://github.com/ueberfuhr-trainings/spring-boot-kafka/compare/main...feature/producer)) —
siehe auch die zugehörigen Pull Requests.

### Story 2: Kafka-Consumer (statistics-service-provider)

> _Durch die Benachrichtigung über Kundenänderungen sollen Statistiken über Kunden aktualisiert werden._

Du implementierst im **statistics-service-provider** einen Kafka-Consumer, der die Nachrichten
vom Kafka-Topic empfängt und daraus Kundenstatistiken ableitet und aktualisiert.

**Musterlösung:** Branch [`feature/consumer`](https://github.com/ueberfuhr-trainings/quarkus-kafka/compare/main...feature/consumer)
([Spring Boot](https://github.com/ueberfuhr-trainings/spring-boot-kafka/compare/main...feature/consumer)) —
siehe auch die zugehörigen Pull Requests.

## Übungen

* [Story 1: Kafka-Producer](issues/producer.html) — Kundenänderungen als Events versenden
* [Story 2: Kafka-Consumer](issues/consumer.html) — Kundenstatistiken aus Events ableiten

