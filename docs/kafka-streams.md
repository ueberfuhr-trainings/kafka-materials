# Kafka Streams

> [!NOTE]
> Dieses Dokument erklärt Kafka Streams: was es ist, wofür es eingesetzt wird, welche Konzepte
> dahinterstehen und wie es in Quarkus und Spring Boot verwendet werden kann.

## Einführung

Kafka Streams ist eine **Client-Bibliothek** für die Verarbeitung und Analyse von Daten, die in Kafka-Topics
gespeichert sind. Im Gegensatz zu klassischen Consumer/Producer-Anwendungen, die Nachrichten einzeln lesen
und schreiben, ermöglicht Kafka Streams eine **deklarative Stream-Verarbeitung** mit Operationen wie
Filtern, Transformieren, Aggregieren und Joinen — direkt auf den Kafka-Daten.

### Kafka Streams vs. klassischer Consumer/Producer

| Aspekt                  | Consumer/Producer                            | Kafka Streams                                    |
|-------------------------|----------------------------------------------|--------------------------------------------------|
| **Abstraktionsebene**   | Einzelne Nachrichten lesen/schreiben         | Datenströme als Ganzes verarbeiten               |
| **Programmiermodell**   | Imperativ (Schleifen, manuelle Offsets)       | Deklarativ (DSL mit `filter`, `map`, `groupBy`)  |
| **Zustandsverwaltung**  | Selbst implementieren                        | Eingebaut (State Stores)                         |
| **Exactly Once**        | Selbst über Transaktionen umsetzen           | Eingebaut (`processing.guarantee=exactly_once_v2`) |
| **Skalierung**          | Manuell über Consumer Groups                 | Automatisch über Partitionen                     |
| **Infrastruktur**       | Nur Kafka-Broker nötig                       | Nur Kafka-Broker nötig (kein separater Cluster)  |

Der entscheidende Vorteil: Kafka Streams benötigt **keine zusätzliche Infrastruktur** wie Spark, Flink oder
ein Hadoop-Cluster. Die Anwendung läuft als normaler Java-Prozess und skaliert über die Partitionen der Input-Topics.


## Kernkonzepte

### KStream und KTable

Kafka Streams kennt zwei zentrale Abstraktionen:

**KStream** — ein unbegrenzter Strom von Ereignissen. Jeder Eintrag ist ein unabhängiges Faktum.

```
KStream<String, CustomerEvent>:
  (key=42, value=CustomerCreated)  @T1
  (key=42, value=CustomerRenamed)  @T2
  (key=42, value=CustomerLocked)   @T3
  → Alle drei Einträge sind sichtbar
```

**KTable** — eine Tabelle, die den **aktuellen Zustand pro Schlüssel** repräsentiert.
Neue Einträge mit demselben Schlüssel überschreiben den vorherigen Wert (Changelog-Semantik).

```
KTable<String, Customer>:
  (key=42, value=CustomerCreated)  @T1  → Zustand: {name: "Tom", state: "active"}
  (key=42, value=CustomerRenamed)  @T2  → Zustand: {name: "Thomas", state: "active"}
  (key=42, value=CustomerLocked)   @T3  → Zustand: {name: "Thomas", state: "locked"}
  → Nur der letzte Zustand ist sichtbar
```

> [!NOTE]
> KTable entspricht konzeptionell einer materialisierten Sicht (Materialized View) auf einen Datenstrom.
> Sie wird automatisch aktualisiert, wenn neue Nachrichten im Topic eintreffen.

### State Stores

Kafka Streams kann **lokalen Zustand** verwalten — z.B. für Aggregationen, Zähler oder Joins.
Dieser Zustand wird in sogenannten **State Stores** gehalten, die intern auf RocksDB basieren
und automatisch über Kafka-Topics gesichert werden (Changelog-Topics).

```
Input-Topic ──► Stream-Verarbeitung ──► State Store (lokal, RocksDB)
                                             │
                                             ▼
                                        Changelog-Topic (Backup)
```

Bei einem Neustart wird der State Store automatisch aus dem Changelog-Topic wiederhergestellt.

### Topologie

Eine Kafka-Streams-Anwendung definiert eine **Topologie**: einen gerichteten Graphen aus
Quellen (Source Topics), Verarbeitungsschritten (Processors) und Senken (Sink Topics).

```
Source Topic(s) ──► Processor 1 ──► Processor 2 ──► Sink Topic(s)
                        │
                        ▼
                   State Store
```


## Anwendungsfälle

### Echtzeit-Datentransformation

Events aus einem Topic in ein anderes Format transformieren und in ein Ziel-Topic schreiben:

```java
StreamsBuilder builder = new StreamsBuilder();
builder.<String, CustomerEvent>stream("customer-events")
  .filter((key, event) -> "created".equals(event.eventType()))
  .mapValues(event -> new CustomerNotification(event.customerUuid(), "Willkommen!"))
  .to("notifications");
```

### Aggregationen und Statistiken

Daten in Echtzeit aggregieren — z.B. Anzahl der Kunden pro Status:

```java
builder.<String, CustomerEvent>stream("customer-events")
  .groupBy((key, event) -> event.customer().state())
  .count(Materialized.as("customers-per-state"))
  .toStream()
  .to("customer-statistics");
```

### Stream-Table-Joins

Einen Event-Strom mit einer Tabelle anreichern — z.B. Bestellungen mit aktuellen Kundendaten:

```java
KStream<String, Order> orders = builder.stream("orders");
KTable<String, Customer> customers = builder.table("customers");

orders.join(customers,
    (order, customer) -> new EnrichedOrder(order, customer.name(), customer.state()))
  .to("enriched-orders");
```

### Zeitfenster-Analyse

Events über Zeitfenster aggregieren — z.B. Anzahl der Events pro Minute:

```java
builder.<String, CustomerEvent>stream("customer-events")
  .groupByKey()
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
  .count(Materialized.as("events-per-minute"))
  .toStream()
  .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), count))
  .to("event-rates");
```

### Weitere Anwendungsfälle

- **Daten-Deduplizierung**: Duplikate in einem Strom erkennen und entfernen.
- **Event-Routing**: Events anhand ihres Inhalts auf verschiedene Topics verteilen.
- **Materialized Views**: Automatisch aktualisierte Sichten auf Datenströme bereitstellen.
- **Anomalie-Erkennung**: Ungewöhnliche Muster in Echtzeit-Datenströmen erkennen.
- **Change Data Capture (CDC)**: Datenbankänderungen (z.B. via Debezium) weiterverarbeiten.


## Kafka Streams in Quarkus

Quarkus unterstützt Kafka Streams über die Extension **`quarkus-kafka-streams`**, die auf der
nativen Kafka-Streams-Bibliothek aufbaut und sie in den Quarkus-Lebenszyklus integriert.

### Abhängigkeit

```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-kafka-streams</artifactId>
</dependency>
```

### Topologie definieren

In Quarkus wird die Topologie als CDI-Bean definiert, das eine `Topology` produziert:

```java
@ApplicationScoped
public class CustomerStreamTopology {

  @Produces
  public Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.<String, CustomerEvent>stream("customer-events")
      .filter((key, event) -> event.customer() != null)
      .mapValues(event -> new CustomerSummary(
        event.customerUuid(),
        event.customer().name(),
        event.eventType()
      ))
      .to("customer-summaries");

    return builder.build();
  }
}
```

### Konfiguration

```properties
quarkus.kafka-streams.bootstrap-servers=localhost:9092
quarkus.kafka-streams.application-id=customer-stream-processor
quarkus.kafka-streams.topics=customer-events

# Serialisierung
quarkus.kafka-streams.default.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde
quarkus.kafka-streams.default.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde

# Exactly Once
quarkus.kafka-streams.processing.guarantee=exactly_once_v2
```

### Interaktive Abfragen (Interactive Queries)

Quarkus ermöglicht den Zugriff auf State Stores über REST-Endpunkte:

```java
@Path("/statistics")
public class StatisticsResource {

  @Inject
  KafkaStreams streams;

  @GET
  @Path("/customers-per-state/{state}")
  public long getCustomersPerState(@PathParam("state") String state) {
    ReadOnlyKeyValueStore<String, Long> store =
      streams.store(
        StoreQueryParameters.fromNameAndType(
          "customers-per-state",
          QueryableStoreTypes.keyValueStore()
        )
      );
    Long count = store.get(state);
    return count != null ? count : 0;
  }
}
```

> [!NOTE]
> Details finden sich im [Quarkus Kafka Streams Guide](https://quarkus.io/guides/kafka-streams).

## Kafka Streams in Spring Boot

Spring Boot unterstützt Kafka Streams über **Spring for Apache Kafka** und die Auto-Konfiguration
in `spring-kafka`.

### Abhängigkeit

```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
```

### Topologie definieren

In Spring Boot wird die Topologie über ein `StreamsBuilderFactoryBean` bereitgestellt.
Die einfachste Variante verwendet eine `@Bean`-Methode mit dem `StreamsBuilder`:

```java
@Configuration
@EnableKafkaStreams
public class CustomerStreamTopology {

  @Bean
  public KStream<String, CustomerEvent> customerStream(StreamsBuilder builder) {
    KStream<String, CustomerEvent> stream = builder.stream("customer-events");

    stream
      .filter((key, event) -> event.customer() != null)
      .mapValues(event -> new CustomerSummary(
        event.customerUuid(),
        event.customer().name(),
        event.eventType()
      ))
      .to("customer-summaries");

    return stream;
  }
}
```

### Konfiguration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    streams:
      application-id: customer-stream-processor
      properties:
        # Serialisierung
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        # Exactly Once
        processing.guarantee: exactly_once_v2
```

### Interaktive Abfragen (Interactive Queries)

In Spring Boot erfolgt der Zugriff auf State Stores über die `StreamsBuilderFactoryBean`:

```java
@RestController
@RequestMapping("/statistics")
public class StatisticsController {

  private final StreamsBuilderFactoryBean factoryBean;

  public StatisticsController(StreamsBuilderFactoryBean factoryBean) {
    this.factoryBean = factoryBean;
  }

  @GetMapping("/customers-per-state/{state}")
  public long getCustomersPerState(@PathVariable String state) {
    KafkaStreams streams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, Long> store =
      streams.store(
        StoreQueryParameters.fromNameAndType(
          "customers-per-state",
          QueryableStoreTypes.keyValueStore()
        )
      );
    Long count = store.get(state);
    return count != null ? count : 0;
  }
}
```

> [!NOTE]
> Details finden sich in der [Spring for Apache Kafka Streams-Dokumentation](https://docs.spring.io/spring-kafka/reference/streams.html).


## Wann Kafka Streams, wann klassischer Consumer?

| Szenario                                                | Kafka Streams | Consumer/Producer |
|---------------------------------------------------------|---------------|-------------------|
| Einfaches Lesen und Verarbeiten einzelner Nachrichten   | ❌             | ✅                 |
| Transformation und Weiterleitung an anderes Topic       | ✅             | ✅                 |
| Aggregationen (Zählen, Summen, Durchschnitte)           | ✅             | ⚠️ (aufwändig)    |
| Joins zwischen Topics                                   | ✅             | ⚠️ (aufwändig)    |
| Zeitfenster-Analyse                                     | ✅             | ⚠️ (aufwändig)    |
| Zustandsbehaftete Verarbeitung mit State Stores         | ✅             | ❌                 |
| Interaktion mit externen Systemen (DB, REST-API)        | ⚠️ (nicht ideal) | ✅              |
| Exactly-Once ohne manuellen Transaktionscode            | ✅             | ❌                 |

### Best Practices

1. **Kafka Streams für Kafka-zu-Kafka-Verarbeitung**: Kafka Streams ist optimal, wenn Daten aus Kafka
   gelesen, verarbeitet und wieder nach Kafka geschrieben werden. Für Interaktionen mit externen
   Systemen (Datenbanken, APIs) ist ein klassischer Consumer oft besser geeignet.

2. **Partitionierung beachten**: Kafka Streams skaliert über die Partitionen des Input-Topics.
   Mehr Partitionen ermöglichen mehr Parallelität, aber auch mehr Overhead.

3. **`application.id` als Consumer Group**: Die `application.id` wird intern als Consumer-Group-ID
   verwendet. Alle Instanzen mit derselben ID teilen sich die Partitionen.

4. **Serdes konfigurieren**: Kafka Streams benötigt Serializer/Deserializer (Serdes) für
   Schlüssel und Werte. Diese sollten explizit konfiguriert werden, um Laufzeitfehler zu vermeiden.

5. **State Store Cleanup**: Bei Änderungen an der Topologie kann es nötig sein, die lokalen
   State Stores zu bereinigen. Die Methode `KafkaStreams.cleanUp()` hilft dabei — aber nur
   aufrufen, wenn die Anwendung gestoppt ist.

6. **Monitoring**: Kafka Streams bietet eingebaute Metriken, die über JMX oder Micrometer
   exponiert werden können. Den Consumer Lag und die State-Store-Größe überwachen.
