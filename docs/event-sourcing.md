# Kafka und Event Sourcing

> [!NOTE]
> Dieses Dokument erklärt den Zusammenhang zwischen Kafka und Event Sourcing:
> Wo sich die Konzepte überschneiden, wo sie sich unterscheiden und wie sie sich sinnvoll ergänzen können.

## Einführung

Kafka und Event Sourcing werden oft in einem Atemzug genannt — und tatsächlich gibt es konzeptionelle Gemeinsamkeiten.
Beide basieren auf der Idee, **Ereignisse als zentrale Datenstruktur** zu verwenden. Dennoch sind es
grundlegend verschiedene Konzepte mit unterschiedlichen Zielen.

| Aspekt          | Kafka                                          | Event Sourcing                                    |
|-----------------|-------------------------------------------------|---------------------------------------------------|
| **Was ist es?** | Verteilte Event-Streaming-Plattform             | Architekturpattern für Datenpersistenz             |
| **Ziel**        | Events zuverlässig transportieren und verteilen | Den vollständigen Zustandsverlauf eines Objekts speichern |
| **Speicherdauer** | Konfigurierbar (Retention), standardmäßig begrenzt | Permanent — Events werden nie gelöscht          |
| **Fokus**       | Kommunikation zwischen Services                 | Zustandsverwaltung innerhalb eines Services        |


## Was ist Event Sourcing?

Beim klassischen Datenbankansatz (CRUD) wird der **aktuelle Zustand** eines Objekts gespeichert.
Bei einer Änderung wird der alte Zustand überschrieben — die Historie geht verloren.

**Event Sourcing** kehrt dieses Prinzip um: Anstatt den aktuellen Zustand zu speichern,
werden alle **Zustandsänderungen als Events** persistiert. Der aktuelle Zustand ergibt sich
durch Abspielen aller Events in der richtigen Reihenfolge.

### Beispiel: Kundenkonto

**CRUD-Ansatz** — nur der aktuelle Zustand wird gespeichert:

```
customers-Tabelle:
  id=42, name="Tom Mayer", state="locked"
```

Die Information, wann und warum der Kunde gesperrt wurde, ist verloren.

**Event-Sourcing-Ansatz** — alle Änderungen als Events:

```
Event-Store (customer-42):
  1. CustomerCreated    { name: "Tom Mayer", state: "active" }     @2025-01-15
  2. CustomerRenamed    { name: "Thomas Mayer" }                    @2025-03-20
  3. CustomerLocked     { reason: "Zahlungsverzug" }                @2025-06-01
  4. CustomerRenamed    { name: "Tom Mayer" }                       @2025-07-10

Aktueller Zustand (durch Abspielen aller Events):
  → name="Tom Mayer", state="locked"
```

### Vorteile von Event Sourcing

- **Vollständige Audit-Historie**: Jede Änderung ist nachvollziehbar — wann, was und in welcher Reihenfolge.
- **Zeitreisen**: Der Zustand zu jedem beliebigen Zeitpunkt kann rekonstruiert werden.
- **Debugging**: Fehler lassen sich reproduzieren, indem die Events bis zum Fehlerzeitpunkt abgespielt werden.
- **Neue Projektionen**: Neue Sichten auf die Daten können nachträglich aus den Events berechnet werden, ohne bestehende Daten zu migrieren.

### Herausforderungen

- **Komplexität**: Die Zustandsrekonstruktion aus Events ist aufwändiger als ein einfaches `SELECT`.
- **Snapshots**: Bei vielen Events wird ein Snapshot-Mechanismus benötigt, um die Rekonstruktion zu beschleunigen.
- **Schema-Evolution**: Events müssen auch nach Monaten oder Jahren noch lesbar sein — Schema-Änderungen müssen rückwärtskompatibel sein.


## Kafka als Event Store?

Kafka speichert Nachrichten in einem unveränderlichen, geordneten Log — das klingt auf den ersten Blick
genau wie ein Event Store. Tatsächlich gibt es aber wichtige Unterschiede:

### Was Kafka mitbringt

- **Geordnetes, unveränderliches Log**: Nachrichten werden in der Reihenfolge des Eingangs gespeichert und können nicht verändert werden.
- **Partitionierung**: Durch die Wahl des Nachrichtenschlüssels (z.B. Kunden-ID) landen alle Events eines Objekts in derselben Partition — und damit in einer garantierten Reihenfolge.
- **Replay**: Consumer können ab einem beliebigen Offset lesen und damit Events erneut abspielen.
- **Log Compaction**: Kafka kann pro Schlüssel nur das letzte Event behalten — nützlich für Snapshot-artige Szenarien, aber **nicht** für vollständiges Event Sourcing.

### Was Kafka fehlt

| Anforderung                     | Event Store                                   | Kafka                                        |
|---------------------------------|-----------------------------------------------|----------------------------------------------|
| Permanente Speicherung          | Events werden nie gelöscht                    | Retention-Policy löscht alte Nachrichten      |
| Lesen nach Aggregat-ID          | Effizient (Index pro Aggregat)                | Nicht direkt möglich (nur sequentielles Lesen der Partition) |
| Optimistic Concurrency Control  | Eingebaut (erwartete Versionsnummer)          | Nicht vorhanden                              |
| Snapshot-Mechanismus            | Oft integriert                                | Nur über Log Compaction (beschränkt)          |
| Abfrage nach Zeitpunkt          | Zustand zu Zeitpunkt X rekonstruierbar        | Möglich über Offset-by-Timestamp, aber aufwändig |

> [!CAUTION]
> Kafka eignet sich gut als **Transportschicht** für Events, aber nur bedingt als alleiniger **Event Store**.
> Für vollwertiges Event Sourcing empfiehlt es sich, einen dedizierten Event Store zu verwenden.


## Spezialisierte Datenbanken für Event Sourcing

Es gibt Datenbanken und Frameworks, die speziell für Event Sourcing konzipiert sind.
Sie bieten Funktionen, die weder Kafka noch klassische relationale Datenbanken von Haus aus mitbringen:
Lesen nach Aggregat-ID, Optimistic Concurrency Control, Snapshots und Projektions-Management.

| Lösung                                                        | Beschreibung                                                                                                     |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| [EventStoreDB](https://www.eventstore.com/)                  | Open-Source-Datenbank, speziell für Event Sourcing entwickelt. Streams pro Aggregat, eingebaute Projektionen, Subscriptions für Echtzeit-Updates. |
| [Axon Server](https://www.axoniq.io/)                        | Event Store und Message Router in einem. Teil des Axon Frameworks, das CQRS und Event Sourcing in Java/Spring nativ unterstützt. |
| [Marten](https://martendb.io/)                               | .NET-Bibliothek, die PostgreSQL als Event Store und Document Store nutzt. Gute Wahl für .NET-basierte Projekte.  |
| [Eventuate](https://eventuate.io/)                           | Framework für Event Sourcing und CQRS in Java/Spring. Nutzt Kafka als Transport und eine relationale DB als Event Store. |
| **Eigene Event-Tabelle** (relationale DB)                     | Einfachster Ansatz: Eine Tabelle mit Spalten wie `aggregate_id`, `event_type`, `payload`, `version`, `timestamp`. Für viele Anwendungsfälle ausreichend. |

### Eigene Event-Tabelle — Beispiel

Wenn kein spezialisiertes System gewünscht ist, reicht oft eine einfache Tabelle:

```sql
CREATE TABLE domain_events (
  id            BIGSERIAL PRIMARY KEY,
  aggregate_id  UUID NOT NULL,
  event_type    VARCHAR(255) NOT NULL,
  payload       JSONB NOT NULL,
  version       INT NOT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (aggregate_id, version)  -- Optimistic Concurrency
);
```

Der `UNIQUE`-Constraint auf `(aggregate_id, version)` verhindert, dass zwei gleichzeitige Schreibvorgänge
dasselbe Aggregat auf dieselbe Version bringen — eine einfache Form von Optimistic Concurrency Control.


## Wo ergänzen sich Kafka und Event Sourcing?

Die Stärke liegt in der Kombination: Event Sourcing als **internes Persistenzmodell** eines Service,
Kafka als **Transportschicht** für die Kommunikation zwischen Services.

### Architektur

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│     Account Service             │       │     Statistics Service           │
│                                 │       │                                 │
│  Command ──► Event Store (DB)   │       │  Kafka Consumer                 │
│               │                 │       │       │                         │
│               ▼                 │       │       ▼                         │
│  Event-Publikation ──► Kafka ───┼──────►│  Projektion (Read Model)        │
│                                 │       │       │                         │
│  Read Model (Projektion)        │       │       ▼                         │
│                                 │       │  Statistik-DB                   │
└─────────────────────────────────┘       └─────────────────────────────────┘
```

### Producer-Seite: Event Sourcing als Persistenzmodell

Der Producer-Service (z.B. Account Service) verwendet Event Sourcing intern:

1. **Kommando empfangen**: `CreateCustomer`, `LockCustomer`, etc.
2. **Events erzeugen und im Event Store speichern**: `CustomerCreated`, `CustomerLocked` — in einer Datenbanktabelle oder einem dedizierten Event Store.
3. **Events an Kafka publizieren**: Die gespeicherten Events werden (z.B. über das [Transactional Outbox Pattern](exception-handling.html#transactional-outbox)) an Kafka weitergeleitet.

Vorteile:
- Der Service hat eine vollständige Audit-Historie.
- Der aktuelle Zustand kann jederzeit aus den Events rekonstruiert werden.
- Kafka dient nur als Transportkanal — die Speicherhoheit bleibt beim Service.

### Consumer-Seite: Event Sourcing als Read-Model-Aufbau

Der Consumer-Service (z.B. Statistics Service) baut aus den empfangenen Events eine **Projektion** auf:

1. **Events aus Kafka konsumieren**: `CustomerCreated`, `CustomerLocked`, etc.
2. **Projektion aktualisieren**: Eine auf den Anwendungsfall optimierte Sicht (Read Model) in einer Datenbank aufbauen.
3. **Bei Bedarf neu aufbauen**: Durch Replay aller Events (Offset auf Anfang setzen) kann die Projektion jederzeit neu berechnet werden.

```java
// Beispiel: Consumer baut eine Statistik-Projektion auf
public void processEvent(CustomerEvent event) {
  switch (event.eventType()) {
    case "created" -> statisticsRepository.incrementTotal();
    case "deleted" -> statisticsRepository.decrementTotal();
    case "locked"  -> statisticsRepository.incrementLocked();
    // ...
  }
}
```

Dies ist streng genommen kein vollständiges Event Sourcing im Consumer
(die Events werden nicht als primäre Datenquelle gespeichert), aber das Prinzip ist verwandt:
**Der Zustand wird aus einer Folge von Events abgeleitet.**

### Consumer-Seite: Entkopplung durch Event-Speicherung

Ein besonders interessanter Vorteil von Event Sourcing auf Consumer-Seite:
**Die Verarbeitung eines Events wird von der Konsumierung entkoppelt.**

Beim klassischen Ansatz muss der Consumer die vollständige Geschäftslogik **während** des Konsumierens ausführen.
Wenn diese Verarbeitung fehlschlägt oder lange dauert, blockiert sie den Consumer:

```
Klassisch:
  Kafka ──► Consumer ──► Geschäftslogik ──► DB-Schreibvorgang ──► ACK
                              │
                              ▼
                    (komplex, kann fehlschlagen,
                     blockiert den Consumer)
```

Mit Event Sourcing auf Consumer-Seite wird das Konsumieren radikal vereinfacht:
Der Consumer speichert das empfangene Event **zunächst nur in eine lokale Event-Tabelle** und bestätigt sofort.
Die eigentliche Verarbeitung — Projektionen berechnen, Statistiken aktualisieren, Benachrichtigungen auslösen —
erfolgt **asynchron** in einem separaten Schritt.

```
Mit Event-Speicherung:
  Kafka ──► Consumer ──► Event in DB speichern ──► ACK  (schnell, einfach)
                                │
                                ▼  (asynchron, später)
                          Event-Verarbeitung ──► Projektion aktualisieren
                                                 Statistik berechnen
                                                 Benachrichtigung senden
```

#### Vorteile

- **Schnelleres ACK**: Der Consumer bestätigt den Empfang sofort nach dem DB-Insert — die aufwändige Verarbeitung blockiert Kafka nicht.
- **Robustheit**: Selbst wenn die Geschäftslogik fehlschlägt, ist das Event bereits sicher in der lokalen DB gespeichert. Es kann beliebig oft erneut verarbeitet werden, ohne Kafka erneut lesen zu müssen.
- **Unabhängigkeit von Kafka-Retention**: Die Events liegen in der eigenen Datenbank und sind damit unabhängig von Kafkas Retention-Policy dauerhaft verfügbar.
- **Mehrere Projektionen**: Aus denselben gespeicherten Events können verschiedene Sichten (Read Models) unabhängig voneinander aufgebaut werden — auch nachträglich.
- **Einfacheres Retry**: Fehlgeschlagene Verarbeitungen können lokal wiederholt werden, ohne den Kafka-Offset zurücksetzen zu müssen.

#### Beispiel

```java
// Schritt 1: Consumer speichert Event nur in die DB
@Incoming("customers")
@Transactional
public void consume(CustomerEvent event) {
  // Einfacher Insert — schnell und robust
  eventRepository.save(new StoredEvent(
    event.eventType(),
    event.customerUuid(),
    event,
    Instant.now()
  ));
  // ACK erfolgt automatisch nach Rückkehr
}
```

```java
// Schritt 2: Asynchrone Verarbeitung (z.B. per Scheduler oder Event-Listener)
@Scheduled(every = "5s")  // Quarkus
// @Scheduled(fixedRate = 5000)  // Spring Boot
public void processStoredEvents() {
  List<StoredEvent> unprocessed = eventRepository.findUnprocessed();
  for (StoredEvent stored : unprocessed) {
    try {
      projectionService.apply(stored.getPayload());
      stored.markAsProcessed();
    } catch (Exception e) {
      stored.incrementRetryCount();
      // Wird beim nächsten Durchlauf erneut versucht
    }
  }
}
```

#### Wann lohnt sich diese Entkopplung?

| Szenario                                                    | Entkopplung sinnvoll? |
|-------------------------------------------------------------|-----------------------|
| Verarbeitung ist komplex oder kann fehlschlagen              | ✅ Ja                  |
| Mehrere unabhängige Verarbeitungsschritte pro Event          | ✅ Ja                  |
| Events müssen dauerhaft vorgehalten werden (über Retention hinaus) | ✅ Ja            |
| Nachträgliche Neuberechnung von Projektionen gewünscht      | ✅ Ja                  |
| Verarbeitung ist trivial und schnell (z.B. Counter erhöhen) | ❌ Eher nicht          |
| Möglichst niedrige End-to-End-Latenz erforderlich           | ❌ Eher nicht          |

### CQRS und Event Sourcing

Event Sourcing wird häufig zusammen mit **CQRS** (Command Query Responsibility Segregation) eingesetzt:

- **Command-Seite**: Nimmt Kommandos entgegen, erzeugt Events, speichert sie im Event Store.
- **Query-Seite**: Baut aus den Events optimierte Read Models für Abfragen auf.

Kafka fungiert dabei als Verbindung zwischen Command- und Query-Seite — insbesondere wenn diese
in verschiedenen Services oder sogar verschiedenen Datenbanken liegen.

```
Command-Seite                Kafka              Query-Seite
─────────────               ──────              ───────────
  Command
     │
     ▼
  Event Store ──► Publish ──► Topic ──► Consume ──► Read Model (DB)
     │                                                 │
     ▼                                                 ▼
  (Event-Log)                                    (Optimierte Abfragen)
```


## Wann lohnt sich Event Sourcing?

| Szenario                                        | Event Sourcing sinnvoll? |
|-------------------------------------------------|--------------------------|
| Vollständige Audit-Historie erforderlich         | ✅ Ja                     |
| Zeitreisen / Zustand zu Zeitpunkt X             | ✅ Ja                     |
| Nachträgliche Berechnung neuer Projektionen     | ✅ Ja                     |
| Einfache CRUD-Anwendung ohne Audit-Anforderung  | ❌ Eher nicht             |
| Hohe Schreiblast mit einfachem Zustandsmodell   | ❌ Eher nicht             |
| Regulatorische Anforderungen (Nachvollziehbarkeit) | ✅ Ja                  |

### Best Practices

1. **Kafka als Transport, nicht als Event Store**: Kafka eignet sich hervorragend, um Events zwischen
   Services zu verteilen. Für die langfristige Speicherung der Events sollte ein dedizierter Event Store
   oder eine Datenbanktabelle verwendet werden.

2. **Transactional Outbox für die Publikation**: Um sicherzustellen, dass Events sowohl im Event Store
   als auch in Kafka landen, empfiehlt sich das
   [Transactional Outbox Pattern](exception-handling.html#transactional-outbox).

3. **Schema-Evolution einplanen**: Events müssen auch nach langer Zeit noch lesbar sein.
   Ein [Schema Registry](json-schema.html) hilft, die Kompatibilität sicherzustellen.

4. **Snapshots implementieren**: Bei vielen Events pro Aggregat sollte ein Snapshot-Mechanismus
   die Zustandsrekonstruktion beschleunigen.

5. **Consumer-Projektionen idempotent gestalten**: Da Events bei einem Replay erneut verarbeitet werden,
   muss die Projektionslogik idempotent sein — wie generell bei Kafka-Consumern empfohlen
   (siehe [Auslieferungssemantiken](delivery-semantics.html)).
