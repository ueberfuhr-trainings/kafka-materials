# Kafka-Transaktionen

> [!NOTE]
> Dieses Dokument erklärt Kafka-Transaktionen im Detail: was sie sind, wofür sie eingesetzt werden
> und was Entwickler auf Producer- und Consumer-Seite umsetzen müssen.

## Einführung

Kafka-Transaktionen ermöglichen **atomare Schreiboperationen** über mehrere Topics und Partitionen hinweg.
Entweder werden alle Nachrichten einer Transaktion erfolgreich geschrieben — oder keine.

Transaktionen bauen auf dem [idempotenten Producer](delivery-semantics.html#der-idempotente-producer) auf
und erweitern ihn um die Fähigkeit, mehrere Operationen als eine unteilbare Einheit zu behandeln.

### Warum Transaktionen?

Ohne Transaktionen gibt es keine Garantie, dass zusammengehörende Nachrichten atomar geschrieben werden:

```
Producer sendet:
  1. Nachricht an Topic-A  ──► Erfolgreich ✅
  2. Nachricht an Topic-B  ──► Fehlgeschlagen ❌

→ Inkonsistenter Zustand: Topic-A hat die Nachricht, Topic-B nicht.
```

Mit Transaktionen werden beide Nachrichten als Einheit behandelt:

```
Transaktion:
  1. Nachricht an Topic-A  ──► Gepuffert
  2. Nachricht an Topic-B  ──► Gepuffert
  commit()                 ──► Beide atomar sichtbar ✅

Oder bei Fehler:
  abort()                  ──► Keine der Nachrichten sichtbar ❌
```


## Funktionsweise

### Transaktions-Koordinator

Kafka verwendet einen speziellen Broker als **Transaktions-Koordinator**. Dieser verwaltet den Zustand
der Transaktion in einem internen Topic (`__transaction_state`).

Der Ablauf einer Transaktion:

```
1. Producer registriert sich mit seiner transactional.id beim Koordinator.
2. Producer ruft beginTransaction() auf.
3. Producer sendet Nachrichten an beliebige Topics/Partitionen.
   → Die Nachrichten werden geschrieben, aber als "uncommitted" markiert.
4. Producer ruft commitTransaction() auf.
   → Der Koordinator markiert alle Nachrichten als "committed".
   → Consumer mit isolation.level=read_committed sehen die Nachrichten jetzt.

Alternativ:
4. Producer ruft abortTransaction() auf.
   → Alle Nachrichten der Transaktion werden verworfen.
```

### Die `transactional.id`

Jeder transaktionale Producer benötigt eine eindeutige **`transactional.id`**. Diese ID dient dazu:

- Den Producer über Neustarts hinweg zu identifizieren (im Gegensatz zur flüchtigen Producer-ID des idempotenten Producers).
- **Zombie-Fencing**: Wenn ein neuer Producer mit derselben `transactional.id` startet, werden offene
  Transaktionen des alten Producers automatisch abgebrochen. Dies verhindert, dass nach einem Crash
  ein alter Producer-Prozess noch Nachrichten in eine laufende Transaktion schreibt.

> [!CAUTION]
> Die `transactional.id` muss für jeden Producer **eindeutig** sein, aber über Neustarts hinweg **stabil** bleiben.
> Wird bei jedem Start eine neue ID generiert, funktioniert das Zombie-Fencing nicht.


## Anwendungsfälle

### Read-Process-Write (Consume-Transform-Produce)

Der häufigste Anwendungsfall: Nachrichten aus einem Topic lesen, verarbeiten und das Ergebnis
in ein anderes Topic schreiben — alles als atomare Operation.

```
Input-Topic ──► Consumer liest Nachricht
                    │
                    ▼
              Verarbeitung / Transformation
                    │
                    ▼
              Producer schreibt Ergebnis ──► Output-Topic
                    │
                    ▼
              Offset-Commit (innerhalb derselben Transaktion)
```

Durch das Committen des Consumer-Offsets innerhalb der Producer-Transaktion wird sichergestellt:
- Die Ausgabe-Nachricht wird **genau dann** sichtbar, wenn der Input-Offset committet wird.
- Bei einem Fehler werden weder die Ausgabe-Nachricht noch der Offset-Commit wirksam.
- Es entsteht **Exactly-Once-Semantik** innerhalb von Kafka.

### Atomares Schreiben in mehrere Topics

Wenn zusammengehörende Nachrichten in verschiedene Topics geschrieben werden müssen:

```
Transaktion:
  send("order-events", orderCreated)
  send("inventory-events", stockReserved)
  send("notification-events", confirmationRequested)
  commit()
```

Entweder sind alle drei Events sichtbar — oder keines.

### Kafka Streams

Kafka Streams nutzt Transaktionen intern für die `exactly_once`-Verarbeitungsgarantie.
Die Konfiguration ist hier besonders einfach, da Kafka Streams die Transaktionslogik vollständig kapselt.


## Was muss der Entwickler tun?

### Producer-Seite

Der Producer muss:

1. Eine **`transactional.id`** konfigurieren.
2. **`initTransactions()`** einmalig aufrufen (beim Start).
3. Nachrichten innerhalb von **`beginTransaction()`** / **`commitTransaction()`** senden.
4. Bei Fehlern **`abortTransaction()`** aufrufen.

> [!NOTE]
> Wenn `transactional.id` gesetzt ist, wird `enable.idempotence` automatisch auf `true` gesetzt
> und `acks` automatisch auf `all`. Diese Werte können nicht überschrieben werden.

#### Kafka-Client-API (direkt)

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-app-tx-1");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

KafkaProducer<String, CustomerEvent> producer = new KafkaProducer<>(props);

// Einmalig beim Start
producer.initTransactions();

try {
  producer.beginTransaction();
  producer.send(new ProducerRecord<>("customers", key, event1));
  producer.send(new ProducerRecord<>("audit-log", key, auditEntry));
  producer.commitTransaction();
} catch (Exception e) {
  producer.abortTransaction();
  throw e;
}
```

#### Quarkus

In Quarkus mit SmallRye Reactive Messaging werden Transaktionen über die `KafkaTransactions`-API gesteuert:

```java
@Inject
@Channel("customers")
KafkaTransactions<CustomerEvent> kafkaTransactions;

public Uni<Void> processAndSend(CustomerEvent event) {
  return kafkaTransactions.withTransaction(emitter -> {
    // Alle Nachrichten innerhalb dieses Blocks sind Teil einer Transaktion
    emitter.send(KafkaRecord.of(event.uuid(), event));
    // Weitere Sends möglich...
    return Uni.createFrom().voidItem();
  });
}
```

**Konfiguration:**

```properties
mp.messaging.outgoing.customers.transactional.id=my-app-tx
```

> [!NOTE]
> Details zur Transaktions-API finden sich in der
> [SmallRye Kafka Transactions-Dokumentation](https://smallrye.io/smallrye-reactive-messaging/latest/kafka/kafka-transactions/).

#### Spring Boot

In Spring Boot werden Transaktionen über den `KafkaTemplate` und den `KafkaTransactionManager` gesteuert:

```java
@Service
public class CustomerEventProducer {

  private final KafkaTemplate<String, CustomerEvent> kafkaTemplate;

  public CustomerEventProducer(KafkaTemplate<String, CustomerEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendTransactional(CustomerEvent event, AuditEntry audit) {
    kafkaTemplate.executeInTransaction(ops -> {
      ops.send("customers", event.uuid(), event);
      ops.send("audit-log", event.uuid(), audit);
      return true;
    });
  }
}
```

Alternativ mit `@Transactional`:

```java
@Service
public class CustomerEventProducer {

  private final KafkaTemplate<String, CustomerEvent> kafkaTemplate;

  public CustomerEventProducer(KafkaTemplate<String, CustomerEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Transactional
  public void sendTransactional(CustomerEvent event, AuditEntry audit) {
    kafkaTemplate.send("customers", event.uuid(), event);
    kafkaTemplate.send("audit-log", event.uuid(), audit);
  }
}
```

**Konfiguration:**

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: my-app-tx-
```

> [!NOTE]
> Durch das Setzen von `transaction-id-prefix` wird automatisch ein `KafkaTransactionManager` erstellt.
> `@Transactional` auf Methoden nutzt dann diesen Manager.
> Details finden sich in der [Spring Kafka Transactions-Dokumentation](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html).

### Consumer-Seite

Der Consumer muss sicherstellen, dass er nur **erfolgreich committete Nachrichten** liest.
Dafür wird die **Isolation Level**-Einstellung verwendet:

| `isolation.level`  | Verhalten                                                                   |
|--------------------|-----------------------------------------------------------------------------|
| `read_uncommitted` | (Standard) Alle Nachrichten werden gelesen, auch aus laufenden Transaktionen. |
| `read_committed`   | Nur Nachrichten aus erfolgreich committeten Transaktionen werden gelesen.   |

> [!CAUTION]
> Der Standardwert ist `read_uncommitted`. Ohne explizite Konfiguration sieht der Consumer
> auch Nachrichten aus abgebrochenen Transaktionen — die Transaktionsgarantie wird damit unterlaufen.

#### Quarkus

```properties
mp.messaging.incoming.customers.isolation.level=read_committed
mp.messaging.incoming.customers.enable.auto.commit=false
```

#### Spring Boot

```yaml
spring:
  kafka:
    consumer:
      isolation-level: read_committed
      enable-auto-commit: false
```

### Read-Process-Write: Offset-Commit in der Transaktion

Für echte Exactly-Once-Semantik muss der Consumer-Offset **innerhalb** der Producer-Transaktion committet werden.
Dadurch wird sichergestellt, dass das Lesen (Offset-Commit) und das Schreiben (neue Nachrichten) atomar sind.

#### Kafka-Client-API (direkt)

```java
// Consumer liest Nachrichten
ConsumerRecords<String, CustomerEvent> records = consumer.poll(Duration.ofMillis(100));

producer.beginTransaction();
try {
  for (ConsumerRecord<String, CustomerEvent> record : records) {
    // Verarbeitung
    ProducerRecord<String, Result> result = process(record);
    producer.send(result);
  }

  // Consumer-Offsets innerhalb der Transaktion committen
  Map<TopicPartition, OffsetAndMetadata> offsets = computeOffsets(records);
  producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

  producer.commitTransaction();
} catch (Exception e) {
  producer.abortTransaction();
}
```

#### Quarkus

SmallRye Reactive Messaging übernimmt das Offset-Commit innerhalb der Transaktion automatisch,
wenn `KafkaTransactions` verwendet wird:

```java
@Inject
@Channel("output")
KafkaTransactions<Result> kafkaTransactions;

@Incoming("input")
public Uni<Void> process(Message<CustomerEvent> message) {
  return kafkaTransactions.withTransaction(message, emitter -> {
    // message wird automatisch innerhalb der Transaktion bestätigt
    Result result = transform(message.getPayload());
    emitter.send(KafkaRecord.of(result.key(), result));
    return Uni.createFrom().voidItem();
  });
}
```

#### Spring Boot

Spring Kafka bietet Unterstützung für Read-Process-Write mit dem `KafkaTransactionManager`:

```java
@Service
public class CustomerEventProcessor {

  private final KafkaTemplate<String, Result> kafkaTemplate;

  public CustomerEventProcessor(KafkaTemplate<String, Result> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @KafkaListener(topics = "input", groupId = "processor")
  @Transactional
  public void process(ConsumerRecord<String, CustomerEvent> record) {
    Result result = transform(record.value());
    kafkaTemplate.send("output", result.key(), result);
    // Offset-Commit erfolgt automatisch am Ende der Transaktion
  }
}
```

**Konfiguration:**

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: processor-tx-
    consumer:
      isolation-level: read_committed
      enable-auto-commit: false
    listener:
      ack-mode: record
```


## Performance-Überlegungen

Transaktionen haben Auswirkungen auf die Performance:

| Aspekt              | Ohne Transaktion     | Mit Transaktion                         |
|---------------------|----------------------|-----------------------------------------|
| Latenz              | Niedrig              | Höher (Koordination mit Transaction Coordinator) |
| Durchsatz           | Hoch                 | Niedriger (Overhead pro Transaktion)    |
| Broker-Last         | Normal               | Höher (zusätzliches internes Topic)     |
| Consumer-Latenz     | Sofort sichtbar      | Erst nach Commit sichtbar               |

### Optimierungstipps

- **Nachrichten bündeln**: Mehrere Nachrichten pro Transaktion senden, anstatt für jede Nachricht
  eine eigene Transaktion zu eröffnen. Der Overhead entsteht pro Transaktion, nicht pro Nachricht.
- **`transaction.timeout.ms`** anpassen: Standardmäßig 60 Sekunden. Bei langen Verarbeitungen erhöhen,
  bei kurzen reduzieren, um abgebrochene Transaktionen schneller zu erkennen.
- **Nur bei Bedarf verwenden**: Für einfache Szenarien ohne Multi-Topic-Schreibvorgänge reicht
  oft der idempotente Producer mit At-Least-Once-Semantik aus.


## Einschränkungen

- **Nur innerhalb von Kafka**: Transaktionen garantieren Atomarität nur für Kafka-Operationen.
  Schreibvorgänge in externe Systeme (Datenbanken, APIs) sind nicht Teil der Transaktion.
  Für End-to-End-Konsistenz mit externen Systemen wird das
  [Transactional Outbox Pattern](exception-handling.html#transactional-outbox) empfohlen.

- **Eine `transactional.id` pro Producer-Instanz**: In einer Microservice-Umgebung mit mehreren
  Instanzen muss jede Instanz eine eigene `transactional.id` verwenden (z.B. mit einem Instanz-Suffix).

- **Kein Mixing von transaktionalen und nicht-transaktionalen Writes**: Ein Producer, der mit
  `transactional.id` konfiguriert ist, muss **alle** Nachrichten innerhalb von Transaktionen senden.

- **Consumer-Gruppen und Rebalancing**: Bei einem Rebalancing können laufende Transaktionen abgebrochen werden.
  Die Anwendung muss damit umgehen können.


## Zusammenfassung

| Aspekt                        | Ohne Transaktion               | Mit Transaktion                            |
|-------------------------------|--------------------------------|--------------------------------------------|
| **Atomarität**                | Pro Nachricht                  | Über mehrere Nachrichten/Topics hinweg     |
| **Exactly-Once-Semantik**     | Nur mit idempotentem Producer (innerhalb einer Partition) | Über Topics und Partitionen hinweg |
| **Zombie-Schutz**             | Nein                           | Ja (über `transactional.id`)               |
| **Consumer-Offset-Commit**    | Separat                        | Atomar mit den geschriebenen Nachrichten   |
| **Voraussetzungen (Producer)**| `enable.idempotence=true`      | `transactional.id` + `acks=all` (automatisch) |
| **Voraussetzungen (Consumer)**| —                              | `isolation.level=read_committed`           |
