---
layout: default
title: "Story 2: Kafka-Consumer"
---

# Kafka-Consumer: Kundenstatistiken aus Events ableiten

Durch die Benachrichtigung über Kundenänderungen sollen Statistiken über Kunden aktualisiert werden.
In dieser Übung implementierst Du im **statistics-service-provider** einen Kafka-Consumer, der
Nachrichten vom Kafka-Topic empfängt und daraus Kundenstatistiken ableitet und pflegt.

## 🎯 Lernziele

* Du verstehst, wie ein Kafka-Consumer in eine bestehende Anwendung integriert wird.
* Du kannst die notwendigen Abhängigkeiten und die Konfiguration für einen Kafka-Consumer einrichten.
* Du kannst einen Listener implementieren, der Nachrichten von einem Kafka-Topic empfängt und verarbeitet.
* Du verstehst die Konzepte Consumer Group, Offset und Deserialisierung im Kontext von Kafka.
* Du kannst aus eingehenden Events abgeleitete Daten (Statistiken) berechnen und persistieren.

## ✅ Definition of Done

* [ ] Die Kafka-Dependencies sind im Projekt eingetragen.
* [ ] Die Kafka-Konfiguration (Bootstrap-Server, Topic-Name, Consumer Group) ist in den
  Anwendungseigenschaften hinterlegt.
* [ ] Ein Kafka-Listener empfängt die Nachrichten vom Kunden-Topic und aktualisiert die Statistiken.
* [ ] Die Statistiken sind über eine REST-Schnittstelle abrufbar.
* [ ] Die Anwendung startet fehlerfrei mit einem laufenden Kafka-Broker.
* [ ] _(Optional)_ Es gibt einen automatisierten Test, der den Empfang und die Verarbeitung von
  Kafka-Nachrichten verifiziert.

## 🪜 Arbeitsschritte

1. **Dependencies ergänzen:** Füge die Kafka-Abhängigkeit zu Deinem Projekt hinzu.
   - _Quarkus:_ SmallRye Reactive Messaging für Kafka
   - _Spring Boot:_ Spring for Apache Kafka (`spring-kafka`)
2. **Konfiguration anlegen:** Trage die Kafka-Verbindungsdaten (Bootstrap-Server, Topic-Name,
   Consumer-Group-ID) in die Anwendungskonfiguration ein (`application.properties` bzw. `application.yml`).
   Konfiguriere die Deserialisierung passend zum Format des Producers.
3. **Nachrichtenformat abbilden:** Erstelle eine Klasse (z.B. ein Record), die das Nachrichtenformat
   des Kafka-Topics abbildet — passend zum Format, das der Producer sendet.
4. **Kafka-Listener implementieren:** Schreibe eine Komponente, die Nachrichten vom Kafka-Topic
   empfängt und die Statistiken aktualisiert.
   - _Quarkus:_ Nutze `@Incoming` (SmallRye Reactive Messaging).
   - _Spring Boot:_ Nutze `@KafkaListener`.
5. **Statistik-Logik umsetzen:** Implementiere die Logik, die aus den eingehenden Events die
   Kundenstatistiken berechnet und speichert (z.B. Anzahl der Kunden, Anzahl nach Status).
6. **REST-Endpunkt bereitstellen:** Stelle die Statistiken über einen REST-Endpunkt zur Verfügung.
7. **Testen:** Starte beide Services zusammen mit einem Kafka-Broker und prüfe, ob Änderungen am
   Kunden im account-service-provider die Statistiken im statistics-service-provider aktualisieren.
8. _(Optional)_ **Automatisierte Tests schreiben:** Schreibe einen Integrationstest, der verifiziert,
   dass eingehende Kafka-Nachrichten korrekt verarbeitet werden.

> 💡 **Tipp:** Der statistics-service-provider muss nicht die vollständige Kundeninformation speichern —
> es reicht, die für die Statistik relevanten Daten zu aggregieren.

**Musterlösung:** Die Pull Requests zeigen die vollständige Implementierung:
[Quarkus](https://github.com/ueberfuhr-trainings/quarkus-kafka/compare/main...feature/consumer) ·
[Spring Boot](https://github.com/ueberfuhr-trainings/spring-boot-kafka/compare/main...feature/consumer)

## 📚 Selbstlernmaterial

**Allgemein:**

* [Apache Kafka Documentation — Consumer API](https://kafka.apache.org/documentation/#consumerapi) —
  Offizielle Dokumentation der Consumer-API
* [Apache Kafka Documentation — Consumer Groups](https://kafka.apache.org/documentation/#intro_consumers) —
  Erklärung von Consumer Groups und Partitionierung

**Quarkus:**

* [Quarkus — Apache Kafka with Reactive Messaging](https://quarkus.io/guides/kafka) —
  Offizieller Quarkus-Guide für Kafka mit SmallRye Reactive Messaging
* [Quarkus — Getting Started with Reactive Messaging](https://quarkus.io/guides/kafka-reactive-getting-started) —
  Einstieg in Reactive Messaging mit Kafka

**Spring Boot:**

* [Spring Boot Reference — Kafka](https://docs.spring.io/spring-boot/reference/messaging/kafka.html) —
  Kafka-Abschnitt der offiziellen Spring-Boot-Dokumentation
* [Spring for Apache Kafka — Reference](https://docs.spring.io/spring-kafka/reference/) —
  Ausführliche Referenz zu KafkaListener, Consumer-Konfiguration und Deserialisierung

## 🤔 Reflexionsfragen

* Was ist eine Consumer Group und warum ist sie wichtig? Was passiert, wenn zwei Instanzen des
  statistics-service-provider dieselbe Group-ID verwenden — und was, wenn sie unterschiedliche verwenden?
* Was passiert mit Nachrichten, die gesendet wurden, bevor der Consumer gestartet war?
  Welche Rolle spielt die Offset-Konfiguration (`auto.offset.reset`)?
* Wie gehst Du damit um, wenn eine Nachricht nicht verarbeitet werden kann (z.B. wegen eines
  Deserialisierungsfehlers)? Welche Strategien gibt es?
* Warum ist es wichtig, dass der Consumer idempotent arbeitet — also dieselbe Nachricht mehrfach
  verarbeiten kann, ohne das Ergebnis zu verfälschen?
* Welche Herausforderungen ergeben sich, wenn die Statistiken nicht nur gezählt, sondern auch
  historisch nachvollziehbar sein sollen (Event Sourcing)?
