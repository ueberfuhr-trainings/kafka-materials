# Kafka und Fehlerbehandlung

> [!NOTE]
> Dieses Dokument beschreibt Best Practices und Techniken zur Fehlerbehandlung in Kafka-basierten
> Messaging-Systemen.

## Inhaltsverzeichnis

- [Einführung](#einführung)
- [Fehlerbehandlungsstrategien für Producer](#fehlerbehandlungsstrategien-für-producer)
  - [Acknowledgement-Modus (Producer-Seite)](#acknowledgement-modus-producer-seite)
  - [Transactional Outbox](#transactional-outbox)
- [Fehlerbehandlungsstrategien für Consumer](#fehlerbehandlungsstrategien-für-consumer)

## Einführung

Kafka ist eine verteilte Event-Streaming-Plattform, die häufig für reaktive Systeme eingesetzt wird.
Die Nachrichtenverarbeitung kann aufgrund temporärer Probleme, Validierungsfehler oder Ausfälle nachgelagerter Services fehlschlagen. Eine ordnungsgemäße Fehlerbehandlung gewährleistet:

- Nachrichtenzuverlässigkeit
- Fehlertoleranz
- Systemstabilität


## Fehlerbehandlungsstrategien für Producer

- **[Acknowledgement-Modus (Producer-Seite)](#acknowledgement-modus-producer-seite):** Steuert, wann eine Nachricht als erfolgreich gesendet gilt.
- **[Transactional Outbox](#transactional-outbox):** Stellt Atomarität zwischen Datenbankoperationen und Kafka-Events sicher.


### Acknowledgement-Modus (Producer-Seite)

Beim Senden von Nachrichten in Kafka bestimmt der Acknowledgement-Modus, wie viele Kafka-Broker den Empfang einer Nachricht bestätigen müssen, bevor der Producer sie als _"erfolgreich gesendet"_ betrachtet.

Dies beeinflusst direkt:
- Zustellungsgarantien
- Performance und Latenz
- Fehlertoleranz

Folgende Optionen stehen zur Verfügung:

| Option          | Beschreibung                                                              | Zuverlässigkeit | Latenz       |
|-----------------|---------------------------------------------------------------------------|-----------------|--------------|
| `all` (oder `-1`) | Wartet, bis alle In-Sync-Replicas (ISRs) bestätigt haben              | ✅ höchste       | 🐢 langsamste |
| `1`             | (Standard) Leader-Broker bestätigt Empfang vor Replikation.              | ⚠️ teilweise    | ⚡️ schnell    |
| `0`             | Fire and Forget — Producer wartet nicht auf Broker-Bestätigung.          | ❌ keine         | ⚡⚡ schnellste |

#### Quarkus-Konfiguration

```properties
mp.messaging.outgoing.customers.acks=all
```

> [!NOTE]
> Weitere Details finden sich im [Quarkus - Kafka Connector](https://quarkus.io/guides/kafka#write-acknowledgement) Guide.

#### Spring-Boot-Konfiguration

```yaml
spring:
  kafka:
    producer:
      acks: all
```

> [!NOTE]
> Weitere Details finden sich im Guide [Spring for Apache Kafka - Sending Messages](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html).


### Transactional Outbox

Das Transactional-Outbox-Pattern löst ein klassisches Problem:

> "Wie stelle ich sicher, dass bei einer Datenbankänderung das zugehörige Kafka-Event ebenfalls gesendet wird — genau einmal —
> selbst wenn mein Service mittendrin abstürzt?"

Dieses Pattern stellt Atomarität zwischen Datenbankänderungen und Nachrichtenversand sicher, ohne auf
verteilte Transaktionen angewiesen zu sein (die komplex und langsam sind).

#### 💡 Das Problem

Stellen wir uns folgenden Code vor (ein naiver Ansatz):

**Quarkus:**

```java
@Transactional
public void createCustomer(Customer customer) {
  customerRepository.persist(customer);
  kafkaEmitter.send(customerEvent); // <-- kann fehlschlagen!
}
```

**Spring Boot:**

```java
@Transactional
public void createCustomer(Customer customer) {
  customerRepository.save(customer);
  kafkaTemplate.send("customers", customerEvent); // <-- kann fehlschlagen!
}
```

Falls Kafka vorübergehend nicht verfügbar ist, wird der Kunde in der DB gespeichert,
aber das Event geht verloren — das System wird inkonsistent.

#### 🧩 Die Lösung

Anstatt direkt an Kafka in derselben Transaktion zu senden,
speichern wir das Event in einer "Outbox"-Tabelle innerhalb derselben Datenbanktransaktion wie unsere Geschäftsdaten.

Später liest ein Hintergrundprozess, Sidecar-Container oder ein anderer Poller die ungesendeten Events aus dieser Tabelle,
sendet sie an Kafka und markiert sie anschließend als gesendet.

#### ✅ Garantien

- **Atomarität**: Datenbankänderung und Event-Speicherung erfolgen in einer Transaktion.
- **Zuverlässigkeit**: Falls der Kafka-Versand fehlschlägt, bleibt das Event in der DB, bis es erneut versucht wird.
- **Idempotenz**: Der Poller kann das Senden sicher wiederholen, ohne Duplikate zu erzeugen (mit `enable.idempotence`=true beim Kafka-Producer).

> [!NOTE]
> Weder Quarkus noch Spring Boot unterstützen das Transactional-Outbox-Pattern direkt, aber es lässt sich leicht manuell implementieren.
> Eine Alternative wäre der Einsatz von [Debezium](https://debezium.io/documentation/reference/stable/integrations/outbox.html),
> das das Datenbank-Transaktionslog (z.B. MySQL Binlog, PostgreSQL WAL, Oracle Redo Log) überwacht und
> jede Datenänderung automatisch in ein Kafka-Event umwandelt.


## Fehlerbehandlungsstrategien für Consumer

### Quarkus

> [!NOTE]
> Details finden sich im Kapitel [Error Handling Strategies](https://quarkus.io/guides/kafka#error-handling)
> des Quarkus Guide.

### Spring Boot

> [!NOTE]
> Details finden sich in der Dokumentation zu [Spring for Apache Kafka - Error Handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).
