---
layout: default
title: "Story 1: Kafka-Producer"
---

# Kafka-Producer: Kundenänderungen als Events versenden

Wenn ein Kunde angelegt, geändert oder gelöscht wird, soll eine Benachrichtigung per Kafka verschickt werden.
In dieser Übung implementierst Du im **account-service-provider** einen Kafka-Producer, der die bereits
vorhandenen Domain-Events konsumiert und als Nachrichten an ein Kafka-Topic sendet.

## 🎯 Lernziele

* Du verstehst, wie ein Kafka-Producer in eine bestehende Anwendung integriert wird.
* Du kannst erklären, wie Domain-Events (CDI Events / Spring Events) als Brücke zwischen Geschäftslogik
  und Kafka-Schicht dienen.
* Du kannst die notwendigen Abhängigkeiten (Dependencies) für Kafka in Deinem Projekt konfigurieren.
* Du kannst einen Event-Listener implementieren, der Domain-Events empfängt und an ein Kafka-Topic weiterleitet.
* Du verstehst die Bedeutung von Serialisierung und Partition Keys beim Versenden von Kafka-Nachrichten.

## ✅ Definition of Done

* [ ] Die Kafka-Dependencies sind im Projekt eingetragen.
* [ ] Die Kafka-Konfiguration (Bootstrap-Server, Topic-Name) ist in den Anwendungseigenschaften hinterlegt.
* [ ] Ein Event-Listener empfängt die Domain-Events (`CustomerCreatedEvent`, `CustomerReplacedEvent`,
  `CustomerDeletedEvent`) und sendet sie als Nachrichten an ein Kafka-Topic.
* [ ] Die Nachrichten enthalten den Event-Typ, die Kunden-UUID und die relevanten Kundendaten.
* [ ] Die Anwendung startet fehlerfrei mit einem laufenden Kafka-Broker.
* [ ] _(Optional)_ Es gibt einen automatisierten Test, der das Versenden der Kafka-Nachrichten verifiziert.

## 🪜 Arbeitsschritte

1. **Dependencies ergänzen:** Füge die Kafka-Abhängigkeit zu Deinem Projekt hinzu.
   - _Quarkus:_ SmallRye Reactive Messaging für Kafka
   - _Spring Boot:_ Spring for Apache Kafka (`spring-kafka`)
2. **Konfiguration anlegen:** Trage die Kafka-Verbindungsdaten (Bootstrap-Server, Topic-Name) in die
   Anwendungskonfiguration ein (`application.properties` bzw. `application.yml`).
3. **Nachrichtenformat definieren:** Erstelle eine Klasse (z.B. ein Record), die das Nachrichtenformat für
   das Kafka-Topic beschreibt — mit Event-Typ, Kunden-UUID und Kundendaten. Erstelle ggf. einen Mapper,
   der Domain-Events in dieses Format überführt.
4. **Event-Listener implementieren:** Schreibe eine Komponente, die auf die Domain-Events aus dem
   `CustomersService` lauscht und die Nachrichten an das Kafka-Topic sendet.
   - _Quarkus:_ Nutze `@Observes` und einen `Emitter` (SmallRye Reactive Messaging).
   - _Spring Boot:_ Nutze `@EventListener` und `KafkaTemplate`.
5. **Testen:** Starte die Anwendung zusammen mit einem Kafka-Broker (z.B. per Docker Compose) und prüfe,
   ob beim Anlegen/Ändern/Löschen von Kunden Nachrichten im Topic ankommen.
6. _(Optional)_ **Automatisierte Tests schreiben:** Schreibe einen Integrationstest, der verifiziert,
   dass die richtigen Nachrichten gesendet werden.

> 💡 **Tipp:** Schau Dir die bestehende Architektur an — insbesondere die Interceptors und Events im
> `CustomersService`. Die Kafka-Schicht muss lediglich auf die bereits gefeuerten Events reagieren.

**Musterlösung:** Die Pull Requests zeigen die vollständige Implementierung:
[Quarkus](https://github.com/ueberfuhr-trainings/quarkus-kafka/compare/main...feature/producer) ·
[Spring Boot](https://github.com/ueberfuhr-trainings/spring-boot-kafka/compare/main...feature/producer)

## 📚 Selbstlernmaterial

**Allgemein:**

* [Apache Kafka Documentation — Producer API](https://kafka.apache.org/documentation/#producerapi) —
  Offizielle Dokumentation der Producer-API

**Quarkus:**

* [Quarkus — Apache Kafka with Reactive Messaging](https://quarkus.io/guides/kafka) —
  Offizieller Quarkus-Guide für Kafka mit SmallRye Reactive Messaging
* [Quarkus — Getting Started with Reactive Messaging](https://quarkus.io/guides/kafka-reactive-getting-started) —
  Einstieg in Reactive Messaging mit Kafka

**Spring Boot:**

* [Spring Boot Reference — Kafka](https://docs.spring.io/spring-boot/reference/messaging/kafka.html) —
  Kafka-Abschnitt der offiziellen Spring-Boot-Dokumentation
* [Spring for Apache Kafka — Reference](https://docs.spring.io/spring-kafka/reference/) —
  Ausführliche Referenz zu KafkaTemplate, Konfiguration und Serialisierung

## 🤔 Reflexionsfragen

* Warum ist es sinnvoll, die Kafka-Schicht von der Geschäftslogik zu entkoppeln und Domain-Events als
  Brücke zu nutzen — statt direkt aus dem Service an Kafka zu senden?
* Welche Rolle spielt der Partition Key beim Versenden von Nachrichten? Was passiert, wenn Du die
  Kunden-UUID als Key verwendest?
* Was passiert, wenn der Kafka-Broker beim Senden einer Nachricht nicht erreichbar ist?
  Wie könnte man damit umgehen?
* Welche Serialisierungsformate kommen für Kafka-Nachrichten in Frage (z.B. JSON, Avro, Protobuf)?
  Welche Vor- und Nachteile haben sie?
* Was wäre anders, wenn Du statt eines Event-Listeners den Kafka-Producer direkt im Interceptor
  aufrufen würdest? Welche Nachteile hätte das?
