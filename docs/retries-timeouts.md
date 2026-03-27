# Retries und Timeouts

> [!NOTE]
> Dieses Dokument erklärt die wichtigsten Retry- und Timeout-Einstellungen für Kafka-Producer und -Consumer
> und gibt Empfehlungen für deren Konfiguration.

## Einführung

Kafka-Clients kommunizieren über das Netzwerk mit Kafka-Brokern. Dabei können verschiedene Probleme auftreten:
Netzwerkunterbrechungen, überlastete Broker, Leader-Wechsel bei Partitionen oder temporäre Ausfälle.

Die Retry- und Timeout-Konfiguration bestimmt, wie der Client auf solche Situationen reagiert:
- Wie lange wird auf eine Antwort gewartet?
- Wie oft und in welchen Abständen wird ein fehlgeschlagener Versuch wiederholt?
- Wann gibt der Client endgültig auf?

Eine sorgfältige Abstimmung dieser Parameter ist entscheidend für die Balance zwischen
**Zuverlässigkeit** (keine Nachrichten verlieren) und **Reaktionsfähigkeit** (nicht zu lange blockieren).


## Übersicht der Parameter

| Parameter            | Betrifft          | Beschreibung                                                        |
|----------------------|-------------------|---------------------------------------------------------------------|
| Reconnect Backoff    | Producer, Consumer | Wartezeit vor einem erneuten Verbindungsversuch zum Broker          |
| Retry Backoff        | Producer, Consumer | Wartezeit zwischen Wiederholungsversuchen fehlgeschlagener Requests |
| Request Timeout      | Producer, Consumer | Maximale Wartezeit auf eine Antwort des Brokers                    |
| Delivery Timeout     | Producer           | Maximale Gesamtzeit für die Zustellung einer Nachricht             |
| Metadata Refresh     | Producer, Consumer | Intervall für die Aktualisierung der Cluster-Metadaten             |


## Reconnect Backoff

Wenn die Verbindung zu einem Broker verloren geht, versucht der Client, sich erneut zu verbinden.
Der Reconnect Backoff steuert die Wartezeit zwischen diesen Verbindungsversuchen.

### Funktionsweise

- **`reconnect.backoff.ms`**: Initiale Wartezeit vor dem ersten erneuten Verbindungsversuch (Standard: 50ms).
- **`reconnect.backoff.max.ms`**: Maximale Wartezeit, die durch exponentielles Backoff erreicht werden kann (Standard: 1000ms).

Der Client verwendet **exponentielles Backoff**: Nach jedem fehlgeschlagenen Versuch verdoppelt sich die Wartezeit,
bis der Maximalwert erreicht ist. Zusätzlich wird ein zufälliger Jitter (±20%) hinzugefügt, um zu vermeiden,
dass viele Clients gleichzeitig erneut verbinden (_Thundering Herd_).

### Beispiel

Bei `reconnect.backoff.ms=50` und `reconnect.backoff.max.ms=1000`:

| Versuch | Wartezeit (ca.)  |
|---------|------------------|
| 1       | 50ms             |
| 2       | 100ms            |
| 3       | 200ms            |
| 4       | 400ms            |
| 5       | 800ms            |
| 6+      | 1000ms (Maximum) |

### Best Practices

- Für Produktionsumgebungen den Maximalwert auf **1–5 Sekunden** setzen, um Broker nicht mit Verbindungsversuchen zu überfluten.
- Bei großen Clustern mit vielen Clients höhere Werte wählen, um den _Thundering-Herd-Effekt_ zu reduzieren.
- Die Standardwerte sind für die meisten Szenarien ausreichend.


## Retry Backoff

Wenn ein Request an den Broker fehlschlägt (z.B. wegen eines Leader-Wechsels oder einer temporären Nichtverfügbarkeit),
wiederholt der Client den Versuch. Der Retry Backoff steuert die Pause zwischen diesen Wiederholungen.

### Funktionsweise

- **`retry.backoff.ms`**: Initiale Wartezeit zwischen Wiederholungsversuchen (Standard: 100ms).
- **`retry.backoff.max.ms`**: Maximale Wartezeit durch exponentielles Backoff (Standard: 1000ms).
- **`retries`**: Maximale Anzahl der Wiederholungsversuche (Standard: `2147483647`, also quasi unbegrenzt — die tatsächliche Begrenzung erfolgt über `delivery.timeout.ms`).

Auch hier kommt **exponentielles Backoff mit Jitter** zum Einsatz.

### Best Practices

- Die Anzahl der Retries (`retries`) nicht manuell begrenzen — stattdessen **`delivery.timeout.ms`** verwenden, um die Gesamtzeit zu steuern.
- Bei bekannt instabilen Netzwerken den initialen Backoff erhöhen (z.B. 200–500ms).
- Zusammen mit `enable.idempotence=true` verwenden, um trotz Retries Duplikate zu vermeiden.


## Request Timeout

Der Request Timeout definiert, wie lange der Client maximal auf eine Antwort des Brokers wartet,
bevor er den Request als fehlgeschlagen betrachtet.

### Funktionsweise

- **`request.timeout.ms`**: Maximale Wartezeit auf eine einzelne Broker-Antwort (Standard: 30000ms = 30 Sekunden).

Dieser Timeout gilt für jeden einzelnen Request — also auch für jeden Retry-Versuch separat.
Er umfasst die Netzwerk-Latenz, die Verarbeitungszeit im Broker und die Zeit für die Replikation
(bei `acks=all`).

### Best Practices

- Der Wert sollte **größer als die erwartete Replikationszeit** sein, insbesondere bei `acks=all`.
- Für Cross-Datacenter-Replikation oder langsame Netzwerke den Wert erhöhen (z.B. 60 Sekunden).
- Nicht zu niedrig setzen, da sonst bei kurzzeitiger Broker-Last unnötige Retries ausgelöst werden.


## Delivery Timeout

Der Delivery Timeout ist die **übergeordnete Zeitbegrenzung für Producer**: Er definiert die maximale Gesamtzeit,
die ein Producer für die erfolgreiche Zustellung einer Nachricht aufwenden darf — einschließlich aller Retries,
Backoffs und Wartezeiten im internen Puffer.

### Funktionsweise

- **`delivery.timeout.ms`**: Maximale Gesamtzeit für die Zustellung (Standard: 120000ms = 2 Minuten).

Nach Ablauf dieser Zeit wird eine `TimeoutException` ausgelöst, unabhängig davon, wie viele Retries noch möglich wären.

Es gilt die Bedingung:

```
delivery.timeout.ms >= request.timeout.ms + linger.ms
```

### Best Practices

- Dies ist der **wichtigste Timeout für Producer** — er bestimmt, wie lange die Anwendung maximal auf eine Zustellung wartet.
- Den Wert an die **Geschäftsanforderungen** anpassen: Wie lange ist es akzeptabel, auf die Zustellung zu warten?
- Für kritische Nachrichten höhere Werte setzen (z.B. 5 Minuten), um auch längere Broker-Ausfälle zu überbrücken.
- Für zeitkritische Anwendungen niedrigere Werte setzen, um schneller auf Fehler reagieren zu können.


## Metadata Refresh

Kafka-Clients halten lokale Metadaten über das Cluster vor (Broker-Adressen, Partition-Leader, Topic-Konfiguration).
Diese Metadaten müssen regelmäßig aktualisiert werden, damit der Client weiß, an welchen Broker er Nachrichten senden
oder von welchem er konsumieren soll.

### Funktionsweise

- **`metadata.max.age.ms`**: Maximales Alter der Metadaten, bevor ein Refresh erzwungen wird — auch ohne erkennbare Änderungen (Standard: 300000ms = 5 Minuten).

Zusätzlich zum zeitgesteuerten Refresh werden Metadaten auch aktualisiert, wenn:
- Ein Producer eine unbekannte Topic-Partition adressiert.
- Ein Request mit dem Fehler `NOT_LEADER_FOR_PARTITION` beantwortet wird.
- Die Verbindung zu einem Broker verloren geht.

### Best Practices

- Den Standardwert von 5 Minuten beibehalten, sofern kein besonderer Grund dagegen spricht.
- In dynamischen Umgebungen (z.B. häufiges Scaling von Brokern) den Wert auf **1–2 Minuten** reduzieren.
- Nicht zu niedrig setzen, da häufige Metadata-Requests die Broker belasten.


## Zusammenspiel der Parameter

Das folgende Diagramm zeigt, wie die Parameter beim Senden einer Nachricht zusammenwirken:

```
                          delivery.timeout.ms (Gesamtzeit)
├──────────────────────────────────────────────────────────────────────┤

│ Versuch 1              │ Backoff │ Versuch 2              │ Backoff │ Versuch 3 ...
│◄─ request.timeout.ms ─►│◄──────►│◄─ request.timeout.ms ─►│◄──────►│
                          retry.     retry.
                          backoff.   backoff.
                          ms         ms * 2
```

Wenn alle Versuche innerhalb von `delivery.timeout.ms` fehlschlagen, wird eine Exception ausgelöst.


## Konfigurationsbeispiele

### Quarkus-Konfiguration

In Quarkus werden die Kafka-Properties über den SmallRye Kafka Connector konfiguriert:

```properties
# --- Producer ---
# Reconnect Backoff
mp.messaging.outgoing.customers.reconnect.backoff.ms=50
mp.messaging.outgoing.customers.reconnect.backoff.max.ms=1000
# Retry Backoff
mp.messaging.outgoing.customers.retry.backoff.ms=100
mp.messaging.outgoing.customers.retry.backoff.max.ms=1000
mp.messaging.outgoing.customers.retries=2147483647
# Request Timeout
mp.messaging.outgoing.customers.request.timeout.ms=30000
# Delivery Timeout
mp.messaging.outgoing.customers.delivery.timeout.ms=120000
# Metadata Refresh
mp.messaging.outgoing.customers.metadata.max.age.ms=300000

# --- Consumer ---
# Reconnect Backoff
mp.messaging.incoming.customers.reconnect.backoff.ms=50
mp.messaging.incoming.customers.reconnect.backoff.max.ms=1000
# Retry Backoff
mp.messaging.incoming.customers.retry.backoff.ms=100
mp.messaging.incoming.customers.retry.backoff.max.ms=1000
# Request Timeout
mp.messaging.incoming.customers.request.timeout.ms=30000
# Metadata Refresh
mp.messaging.incoming.customers.metadata.max.age.ms=300000
```

> [!NOTE]
> Details zu den Properties finden sich im [Quarkus Kafka Guide](https://quarkus.io/guides/kafka).
> Die Properties werden direkt an den zugrundeliegenden Kafka-Client durchgereicht.

### Spring-Boot-Konfiguration

In Spring Boot können die Properties global oder pro Producer/Consumer konfiguriert werden:

```yaml
spring:
  kafka:
    producer:
      properties:
        # Reconnect Backoff
        reconnect.backoff.ms: 50
        reconnect.backoff.max.ms: 1000
        # Retry Backoff
        retry.backoff.ms: 100
        retry.backoff.max.ms: 1000
        retries: 2147483647
        # Request Timeout
        request.timeout.ms: 30000
        # Delivery Timeout
        delivery.timeout.ms: 120000
        # Metadata Refresh
        metadata.max.age.ms: 300000
    consumer:
      properties:
        # Reconnect Backoff
        reconnect.backoff.ms: 50
        reconnect.backoff.max.ms: 1000
        # Retry Backoff
        retry.backoff.ms: 100
        retry.backoff.max.ms: 1000
        # Request Timeout
        request.timeout.ms: 30000
        # Metadata Refresh
        metadata.max.age.ms: 300000
```

> [!NOTE]
> Die Properties unter `spring.kafka.producer.properties` bzw. `spring.kafka.consumer.properties` werden
> direkt an den zugrundeliegenden Kafka-Client durchgereicht.
> Details finden sich in der [Spring for Apache Kafka-Referenz](https://docs.spring.io/spring-kafka/reference/).

### Beispiel: Robuste Produktionskonfiguration

Für eine Produktionsumgebung mit hohen Zuverlässigkeitsanforderungen:

```properties
# Kafka-Producer-Properties (Quarkus-Notation)
mp.messaging.outgoing.customers.acks=all
mp.messaging.outgoing.customers.enable.idempotence=true
mp.messaging.outgoing.customers.reconnect.backoff.ms=100
mp.messaging.outgoing.customers.reconnect.backoff.max.ms=5000
mp.messaging.outgoing.customers.retry.backoff.ms=200
mp.messaging.outgoing.customers.retry.backoff.max.ms=2000
mp.messaging.outgoing.customers.request.timeout.ms=30000
mp.messaging.outgoing.customers.delivery.timeout.ms=300000
mp.messaging.outgoing.customers.metadata.max.age.ms=120000
```

```yaml
# Kafka-Producer-Properties (Spring Boot-Notation)
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        reconnect.backoff.ms: 100
        reconnect.backoff.max.ms: 5000
        retry.backoff.ms: 200
        retry.backoff.max.ms: 2000
        request.timeout.ms: 30000
        delivery.timeout.ms: 300000
        metadata.max.age.ms: 120000
```

Diese Konfiguration:
- Stellt sicher, dass alle Replicas bestätigen (`acks=all`).
- Aktiviert Idempotenz, um Duplikate bei Retries zu vermeiden.
- Erlaubt bis zu 5 Minuten für die Zustellung (`delivery.timeout.ms=300000`).
- Aktualisiert Metadaten alle 2 Minuten für schnellere Reaktion auf Cluster-Änderungen.
- Verwendet moderate Backoff-Werte, um Broker nicht zu überlasten.
