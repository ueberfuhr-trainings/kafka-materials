# Auslieferungssemantiken

> [!NOTE]
> Dieses Dokument erklärt die drei Auslieferungssemantiken in Kafka — At Most Once, At Least Once und Exactly Once —
> und beschreibt, was Kafka selbst leistet und was Producer und Consumer jeweils umsetzen müssen.

## Einführung

Bei der Übertragung von Nachrichten zwischen verteilten Systemen stellt sich immer die Frage:
**Wie oft wird eine Nachricht zugestellt?** Kafka bietet drei Auslieferungssemantiken,
die sich in ihren Garantien und dem damit verbundenen Aufwand unterscheiden.

| Semantik         | Garantie                                      | Nachrichtenverlust | Duplikate  | Praxisbeispiele                                      |
|------------------|-----------------------------------------------|--------------------|------------|------------------------------------------------------|
| At Most Once     | Nachricht wird **höchstens einmal** zugestellt | Möglich            | Nein       | Logging, Metriken, Sensortelemetrie                  |
| At Least Once    | Nachricht wird **mindestens einmal** zugestellt| Nein               | Möglich    | E-Mail-Benachrichtigungen, Suchindex-Updates         |
| Exactly Once     | Nachricht wird **genau einmal** zugestellt     | Nein               | Nein       | Finanztransaktionen, Kontostandsänderungen           |


## Acknowledgements

Die Bestätigungsmechanismen auf Producer- und Consumer-Seite spielen eine zentrale Rolle für die Auslieferungssemantik.
Eine ausführliche Erklärung findet sich in der [Dokumentation zu Acknowledgements](acks.html).


## At Most Once

Bei At Most Once wird die Nachricht höchstens einmal zugestellt. Falls ein Fehler auftritt,
geht die Nachricht verloren — es wird kein erneuter Versuch unternommen.

### Producer-Seite

Der Producer sendet die Nachricht und wartet nicht auf eine vollständige Bestätigung.
Falls der Versand fehlschlägt, wird **nicht** wiederholt.

**Konfiguration:**

```properties
acks=0          # Keine Bestätigung vom Broker abwarten
retries=0       # Keine Wiederholungsversuche
```

### Consumer-Seite

Der Consumer committet den Offset **vor** der Verarbeitung der Nachricht.
Falls die Verarbeitung fehlschlägt, wird die Nachricht nicht erneut gelesen — sie geht verloren.

```
1. Offset committen  ──►  2. Nachricht verarbeiten
                               │
                               ▼
                          (Fehler? Nachricht verloren)
```

### Wann sinnvoll?

- Wenn vereinzelte Nachrichtenverluste tolerierbar sind.
- Wenn maximale Performance und minimale Latenz gefordert sind.
- Wenn die Daten ohnehin in kurzen Abständen aktualisiert werden (z.B. Sensorwerte).


## At Least Once

Bei At Least Once wird sichergestellt, dass jede Nachricht mindestens einmal zugestellt wird.
Dafür werden Nachrichten bei Fehlern wiederholt — was zu Duplikaten führen kann.

Dies ist die **Standardsemantik** von Kafka.

### Producer-Seite

Der Producer wartet auf die Bestätigung des Brokers und wiederholt den Versand bei Fehlern.

**Konfiguration:**

```properties
acks=all                       # Alle In-Sync-Replicas müssen bestätigen
retries=2147483647             # Quasi unbegrenzte Wiederholungsversuche
delivery.timeout.ms=120000     # Gesamte Zustellzeit begrenzen
```

### Consumer-Seite

Der Consumer committet den Offset **nach** der erfolgreichen Verarbeitung der Nachricht.
Falls die Verarbeitung fehlschlägt oder der Consumer abstürzt, wird die Nachricht erneut gelesen.

```
1. Nachricht verarbeiten  ──►  2. Offset committen
        │
        ▼
   (Fehler/Absturz? Nachricht wird erneut gelesen → Duplikat)
```

### Umgang mit Duplikaten

Da Duplikate auftreten können, muss der Consumer **idempotent** arbeiten:
- Nachrichten anhand einer eindeutigen ID (z.B. Event-UUID) erkennen.
- Bereits verarbeitete Nachrichten in einer Datenbank oder einem Cache vermerken.
- Datenbankoperationen so gestalten, dass wiederholte Ausführung dasselbe Ergebnis liefert
  (z.B. `INSERT ... ON CONFLICT DO NOTHING` oder `UPSERT`).

### Wann sinnvoll?

- Wenn kein Nachrichtenverlust tolerierbar ist.
- Wenn Duplikate durch idempotente Verarbeitung behandelt werden können.
- Für die meisten Anwendungsfälle ist dies die richtige Wahl.


## Exactly Once

Exactly Once stellt sicher, dass jede Nachricht genau einmal verarbeitet wird —
ohne Verlust und ohne Duplikate. Dies ist die stärkste Garantie, erfordert aber den höchsten Aufwand.

### Was Kafka selbst leistet

Kafka bietet seit Version 0.11 Mechanismen für Exactly-Once-Semantik:

1. **Idempotenter Producer**: Verhindert Duplikate auf Broker-Seite.
2. **Transaktionen**: Ermöglichen atomare Schreib-/Lese-Operationen über mehrere Partitionen und Topics hinweg.

> [!CAUTION]
> Exactly Once in Kafka bezieht sich auf die **Kafka-interne Verarbeitung** (Produce → Kafka → Consume).
> Für End-to-End Exactly Once (inklusive externer Systeme wie Datenbanken) ist zusätzliche Logik erforderlich,
> z.B. das [Transactional Outbox Pattern](exception-handling.html#transactional-outbox).


## Der idempotente Producer

Ein idempotenter Producer stellt sicher, dass Nachrichten **auf Broker-Seite nicht dupliziert** werden,
selbst wenn der Producer dieselbe Nachricht mehrfach sendet (z.B. nach einem Netzwerk-Timeout mit anschließendem Retry).

### Funktionsweise

Kafka weist jedem Producer eine eindeutige **Producer-ID (PID)** zu. Jede Nachricht erhält zusätzlich eine
**Sequenznummer** pro Partition. Der Broker erkennt anhand von PID und Sequenznummer, ob eine Nachricht
bereits geschrieben wurde, und verwirft Duplikate.

```
Producer (PID=42)
  │
  ├─► Nachricht (Seq=1) ──► Broker: Akzeptiert ✅
  ├─► Nachricht (Seq=2) ──► Broker: Akzeptiert ✅
  ├─► Nachricht (Seq=2) ──► Broker: Duplikat erkannt, verworfen 🔄
  └─► Nachricht (Seq=3) ──► Broker: Akzeptiert ✅
```

### Einschränkungen

- Idempotenz schützt nur vor Duplikaten **desselben Producers** innerhalb **einer Partition**.
- Sie garantiert **keine** Exactly-Once-Semantik über mehrere Topics oder Partitionen hinweg — dafür werden Transaktionen benötigt.
- Die Producer-ID wird beim Neustart des Producers neu vergeben. Für Schutz über Neustarts hinweg sind Transaktionen erforderlich.


## Kafka-Transaktionen

Transaktionen erweitern die Idempotenz und ermöglichen atomare Operationen:
Entweder werden **alle** Nachrichten einer Transaktion geschrieben — oder **keine**.

Eine ausführliche Erklärung mit Codebeispielen für Quarkus und Spring Boot findet sich in der
[Dokumentation zu Kafka-Transaktionen](transactions.html).


## Konfigurationsbeispiele

### At Most Once

#### Quarkus

```properties
# Producer
mp.messaging.outgoing.customers.acks=0
mp.messaging.outgoing.customers.retries=0

# Consumer
mp.messaging.incoming.customers.enable.auto.commit=true
mp.messaging.incoming.customers.auto.commit.interval.ms=100
```

#### Spring Boot

```yaml
spring:
  kafka:
    producer:
      acks: 0
      retries: 0
    consumer:
      enable-auto-commit: true
      properties:
        auto.commit.interval.ms: 100
```

### At Least Once (empfohlener Standard)

#### Quarkus

```properties
# Producer
mp.messaging.outgoing.customers.acks=all
mp.messaging.outgoing.customers.retries=2147483647
mp.messaging.outgoing.customers.delivery.timeout.ms=120000
mp.messaging.outgoing.customers.enable.idempotence=true

# Consumer
mp.messaging.incoming.customers.enable.auto.commit=false
mp.messaging.incoming.customers.commit-strategy=throttled
```

> [!NOTE]
> In Quarkus mit SmallRye Reactive Messaging wird das Offset-Commit über die `commit-strategy` gesteuert.
> Die Strategie `throttled` committet Offsets periodisch nach erfolgreicher Verarbeitung.
> Details finden sich im [Quarkus Kafka Guide](https://quarkus.io/guides/kafka#consumer-rebalance-listener).

#### Spring Boot

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        delivery.timeout.ms: 120000
        enable.idempotence: true
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: record
```

> [!NOTE]
> In Spring Boot wird das manuelle Commit über den `ack-mode` des Listeners gesteuert.
> `record` committet nach jeder einzelnen Nachricht, `batch` nach jedem Poll-Batch.
> Details finden sich in der [Spring Kafka-Dokumentation](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html).

### Exactly Once (Read-Process-Write)

#### Quarkus

```properties
# Producer (transaktional)
mp.messaging.outgoing.customers.acks=all
mp.messaging.outgoing.customers.enable.idempotence=true
mp.messaging.outgoing.customers.transactional.id=my-app-tx

# Consumer
mp.messaging.incoming.customers.enable.auto.commit=false
mp.messaging.incoming.customers.isolation.level=read_committed
```

> [!NOTE]
> SmallRye Reactive Messaging bietet für Exactly-Once-Szenarien die Möglichkeit,
> [Kafka-Transaktionen](https://smallrye.io/smallrye-reactive-messaging/latest/kafka/kafka-transactions/) zu verwenden.

#### Spring Boot

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
      transaction-id-prefix: my-app-tx-
    consumer:
      enable-auto-commit: false
      isolation-level: read_committed
      properties:
        auto.offset.reset: earliest
    listener:
      ack-mode: record
```

> [!NOTE]
> Spring Kafka unterstützt Exactly-Once-Semantik über `KafkaTransactionManager`.
> Details finden sich in der [Spring Kafka Transactions-Dokumentation](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html).


## Zusammenfassung und Best Practices

### Welche Semantik wählen?

| Anforderung                              | Empfohlene Semantik |
|------------------------------------------|---------------------|
| Maximale Performance, Verlust tolerierbar | At Most Once        |
| Kein Verlust, Duplikate tolerierbar       | At Least Once       |
| Weder Verlust noch Duplikate             | Exactly Once        |

### Best Practices

1. **At Least Once als Standard verwenden**: Für die meisten Anwendungsfälle bietet At Least Once
   das beste Verhältnis aus Zuverlässigkeit und Komplexität.

2. **Idempotenten Producer aktivieren** (`enable.idempotence=true`): Dies verhindert Duplikate
   auf Broker-Seite und hat nur minimale Performance-Kosten. Ab Kafka 3.0 ist dies standardmäßig aktiviert.

3. **Auto-Commit deaktivieren** (`enable.auto.commit=false`): Auto-Commit kann zu unerwartetem
   Nachrichtenverlust führen und sollte in Produktionsumgebungen vermieden werden.

4. **Consumer idempotent gestalten**: Unabhängig von der gewählten Semantik ist es eine Best Practice,
   Consumer idempotent zu implementieren. Dies schützt auch vor Duplikaten durch Consumer-Rebalancing.

5. **`acks=all` für Zuverlässigkeit**: Zusammen mit `min.insync.replicas=2` auf Broker-Seite
   bietet dies den besten Schutz vor Datenverlust.

6. **Exactly Once nur bei Bedarf**: Die transaktionale Verarbeitung hat höhere Latenz und niedrigeren Durchsatz.
   Nur verwenden, wenn die Geschäftsanforderungen dies erfordern.

7. **End-to-End-Semantik beachten**: Kafka garantiert Exactly Once nur innerhalb von Kafka.
   Für externe Systeme (Datenbanken, APIs) muss die Anwendung selbst Idempotenz oder das
   [Transactional Outbox Pattern](exception-handling.html#transactional-outbox) implementieren.
