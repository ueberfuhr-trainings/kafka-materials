# Acknowledgements (ACKs)

> [!NOTE]
> Dieses Dokument erklärt die Bestätigungsmechanismen auf Producer- und Consumer-Seite in Kafka
> und zeigt, wie sie in Quarkus und Spring Boot konfiguriert und implementiert werden.

## Einführung

Sowohl beim Senden als auch beim Empfangen von Nachrichten spielen Bestätigungen (Acknowledgements) eine zentrale Rolle.
Sie steuern, wann eine Nachricht als erfolgreich zugestellt bzw. verarbeitet gilt, und beeinflussen damit direkt
die [Auslieferungssemantik](delivery-semantics.html) (At Most Once, At Least Once, Exactly Once).


## ACKs beim Producer

Der `acks`-Parameter ist eine der wichtigsten Einstellungen für den Producer.
Er bestimmt, wie viele Broker den Empfang einer Nachricht bestätigen müssen,
bevor der Producer sie als erfolgreich gesendet betrachtet.

| `acks`             | Verhalten                                                                                    |
|--------------------|----------------------------------------------------------------------------------------------|
| `0`                | Der Producer wartet **nicht** auf eine Bestätigung. Die Nachricht gilt sofort als gesendet.   |
| `1`                | Der **Leader-Broker** bestätigt den Empfang, bevor die Nachricht an Replicas weitergegeben wird. |
| `all` (oder `-1`)  | **Alle In-Sync-Replicas (ISRs)** müssen den Empfang bestätigen.                              |

### Wie `acks` die Zuverlässigkeit beeinflusst

```
Producer ──► Leader-Broker ──► Replica 1
                           ──► Replica 2

acks=0:   Producer ──► (sendet, wartet nicht)
acks=1:   Producer ──► Leader bestätigt ✅ (Replicas evtl. noch nicht synchron)
acks=all: Producer ──► Leader bestätigt ✅ erst nachdem alle ISRs synchron sind
```

- **`acks=0`**: Schnellste Variante, aber **kein Schutz vor Datenverlust**. Der Producer erfährt nicht, ob die Nachricht angekommen ist.
- **`acks=1`**: Guter Kompromiss aus Geschwindigkeit und Sicherheit. Allerdings kann die Nachricht verloren gehen, wenn der Leader **nach** der Bestätigung, aber **vor** der Replikation ausfällt.
- **`acks=all`**: Höchste Zuverlässigkeit. In Kombination mit `min.insync.replicas=2` (Broker-Einstellung) wird sichergestellt, dass die Nachricht auf mindestens zwei Brokern gespeichert ist, bevor sie bestätigt wird.

### Konfiguration

**Quarkus:**

```properties
mp.messaging.outgoing.customers.acks=all
```

**Spring Boot:**

```yaml
spring:
  kafka:
    producer:
      acks: all
```

### Wie es zu Duplikaten kommen kann

Auch mit `acks=all` können Duplikate entstehen. Das folgende Szenario zeigt, warum:

```
1. Producer sendet Nachricht (Seq=1) an den Leader.
2. Leader schreibt die Nachricht und repliziert sie an alle ISRs.
3. Leader sendet ACK an den Producer.
4. ⚡ Netzwerkfehler: ACK geht verloren und erreicht den Producer nicht.
5. Producer nimmt an, der Versand sei fehlgeschlagen.
6. Producer wiederholt den Versand (Retry) → dieselbe Nachricht wird erneut geschrieben.
```

**Ergebnis**: Die Nachricht existiert nun **zweimal** im Topic — obwohl der Producer sie nur einmal senden wollte.

Dieses Problem tritt immer dann auf, wenn der Producer Retries durchführt und der Broker nicht erkennen kann,
dass eine Nachricht bereits geschrieben wurde. Die Lösung dafür ist der **idempotente Producer**
(`enable.idempotence=true`), der in der [Dokumentation zu Auslieferungssemantiken](delivery-semantics.html) beschrieben wird.


## ACKs beim Consumer

### Standardverhalten

Standardmäßig bestätigt der Consumer den Empfang einer Nachricht **automatisch**, noch bevor die Anwendung
sie verarbeitet hat. Das geschieht über den **Auto-Commit-Mechanismus**: Der Consumer committet den aktuellen
Offset in regelmäßigen Abständen (standardmäßig alle 5 Sekunden), unabhängig davon, ob die Nachricht
erfolgreich verarbeitet wurde.

```
1. Consumer liest Nachricht vom Broker
2. Offset wird automatisch committet (Zeitintervall)
3. Nachricht wird deserialisiert und an die Anwendung übergeben
4. Anwendung verarbeitet die Nachricht
```

> [!CAUTION]
> Bei Auto-Commit kann es passieren, dass der Offset bereits committet wurde, die Verarbeitung aber noch
> nicht abgeschlossen ist. Stürzt der Consumer in diesem Moment ab, geht die Nachricht verloren
> (At-Most-Once-Semantik).

### Manuelles Acknowledgement

Für zuverlässigere Verarbeitung kann der Entwickler das Acknowledgement **manuell** steuern.
Der Offset wird erst committet, wenn die Verarbeitung abgeschlossen ist — z.B. nachdem die Daten
erfolgreich in eine Datenbank geschrieben wurden.

```
1. Consumer liest Nachricht vom Broker
2. Nachricht wird deserialisiert und an die Anwendung übergeben
3. Anwendung verarbeitet die Nachricht (z.B. DB-Schreibvorgang)
4. Anwendung sendet ACK → Offset wird committet ✅
```

Dies ermöglicht **At-Least-Once-Semantik**: Falls die Verarbeitung fehlschlägt oder der Consumer abstürzt,
wird die Nachricht beim nächsten Start erneut gelesen.

### Quarkus: Manuelles Acknowledgement

In Quarkus mit SmallRye Reactive Messaging wird das Acknowledgement über die `Metadata` der Nachricht
oder über die `Message`-API gesteuert:

**Automatisch (Standard):**

```java
@Incoming("customers")
public void consume(CustomerEvent event) {
  // Nachricht wird automatisch bestätigt, wenn die Methode erfolgreich zurückkehrt
  customerService.process(event);
}
```

**Manuell:**

```java
@Incoming("customers")
@Acknowledgment(Acknowledgment.Strategy.MANUAL)
public CompletionStage<Void> consume(Message<CustomerEvent> message) {
  try {
    customerService.process(message.getPayload());
    customerRepository.save(message.getPayload());
    // ACK erst nach erfolgreicher DB-Speicherung
    return message.ack();
  } catch (Exception e) {
    // NACK bei Fehler — Nachricht wird erneut zugestellt
    return message.nack(e);
  }
}
```

**Konfiguration:**

```properties
# Auto-Commit deaktivieren
mp.messaging.incoming.customers.enable.auto.commit=false
# Commit-Strategie: throttled committet periodisch nach erfolgreichem ACK
mp.messaging.incoming.customers.commit-strategy=throttled
```

> [!NOTE]
> SmallRye Reactive Messaging bietet verschiedene Acknowledgement-Strategien:
> `MANUAL`, `PRE_PROCESSING`, `POST_PROCESSING` und `NONE`.
> Details finden sich im [Quarkus Kafka Guide](https://quarkus.io/guides/kafka#acknowledgement).

### Spring Boot: Manuelles Acknowledgement

In Spring Boot wird das Acknowledgement über den `Acknowledgment`-Parameter im Listener gesteuert:

**Automatisch (Standard):**

```java
@KafkaListener(topics = "customers", groupId = "statistics")
public void consume(CustomerEvent event) {
  // Offset wird automatisch committet (je nach ack-mode)
  customerService.process(event);
}
```

**Manuell:**

```java
@KafkaListener(topics = "customers", groupId = "statistics")
public void consume(CustomerEvent event, Acknowledgment ack) {
  try {
    customerService.process(event);
    customerRepository.save(event);
    // ACK erst nach erfolgreicher DB-Speicherung
    ack.acknowledge();
  } catch (Exception e) {
    // Kein ACK — Nachricht wird erneut zugestellt
    throw e;
  }
}
```

**Konfiguration:**

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      # MANUAL: Anwendung muss ack.acknowledge() aufrufen
      ack-mode: manual
```

> [!NOTE]
> Spring Kafka bietet verschiedene `ack-mode`-Optionen:
> `RECORD`, `BATCH`, `TIME`, `COUNT`, `MANUAL` und `MANUAL_IMMEDIATE`.
> Bei `RECORD` und `BATCH` wird automatisch nach Rückkehr der Methode committet.
> Bei `MANUAL` und `MANUAL_IMMEDIATE` muss die Anwendung `ack.acknowledge()` explizit aufrufen.
> Details finden sich in der [Spring Kafka-Dokumentation](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html).


## Was passiert, wenn das ACK vergessen wird?

Wenn der Entwickler manuelles Acknowledgement konfiguriert, aber das ACK nie sendet, hat das schwerwiegende Folgen:

### Auswirkungen

1. **Der Offset wird nicht committet**: Kafka merkt sich nicht, dass die Nachricht verarbeitet wurde.

2. **Bei einem Consumer-Neustart** wird dieselbe Nachricht erneut gelesen und verarbeitet —
   da der letzte committete Offset vor dieser Nachricht liegt.

3. **Wachsender Consumer Lag**: Der Unterschied zwischen dem letzten geschriebenen und dem letzten
   committeten Offset wächst stetig. Monitoring-Tools zeigen einen zunehmenden Lag an.

4. **Bei Consumer-Rebalancing** (z.B. wenn ein neuer Consumer der Gruppe beitritt) werden
   alle nicht-committeten Nachrichten erneut an die Consumer verteilt.

5. **Framework-spezifisches Verhalten**:
   - **Quarkus (SmallRye)**: Bei der `throttled`-Strategie blockiert die Partition, wenn Nachrichten
     nicht bestätigt werden. Neue Nachrichten derselben Partition werden nicht mehr gelesen, bis die
     ausstehenden Acknowledgements erfolgt sind oder ein Timeout eintritt.
   - **Spring Boot**: Bei `ack-mode=MANUAL` wartet der Listener-Container auf das Acknowledgement.
     Ohne ACK werden nachfolgende Nachrichten zwar gelesen, aber der Offset wird nicht fortgeschrieben.
     Bei einem Neustart werden alle unbestätigten Nachrichten erneut verarbeitet.

### Beispiel

```
Consumer liest Nachrichten 1, 2, 3, 4, 5
Consumer bestätigt nur Nachricht 1 und 2
Letzter committeter Offset: 2

→ Consumer-Neustart oder Rebalancing
→ Consumer liest erneut ab Offset 3
→ Nachrichten 3, 4, 5 werden doppelt verarbeitet
```

> [!CAUTION]
> Ein vergessenes ACK fällt nicht sofort auf — die Anwendung läuft scheinbar normal weiter.
> Das Problem zeigt sich erst bei einem Neustart, Rebalancing oder wenn der Consumer Lag im Monitoring auffällt.
> Daher ist es wichtig, den **Consumer Lag** aktiv zu überwachen.


## Zusammenfassung

| Aspekt                 | Producer ACK (`acks`)                          | Consumer ACK (Offset-Commit)                        |
|------------------------|------------------------------------------------|-----------------------------------------------------|
| **Was wird bestätigt?**| Empfang der Nachricht durch den Broker         | Verarbeitung der Nachricht durch die Anwendung      |
| **Wer bestätigt?**     | Broker → Producer                              | Consumer → Kafka (Offset-Commit)                    |
| **Standard**           | `acks=1` (Leader bestätigt)                    | Auto-Commit alle 5 Sekunden                         |
| **Empfehlung**         | `acks=all` für Zuverlässigkeit                 | Manuelles Commit nach erfolgreicher Verarbeitung    |
| **Risiko bei Fehler**  | Nachricht kann verloren gehen oder dupliziert werden | Nachricht wird erneut gelesen (Duplikat) oder geht verloren |
