# Apache AVRO

## 🧩 Was ist Avro?

Avro ist ein Datenserialisierungssystem — ursprünglich von Apache als Teil des Hadoop-Ökosystems entwickelt.
Es definiert beides:
1. Ein kompaktes, schnelles, binäres Format zur Kodierung von Daten.
2. Eine Schema-Sprache (üblicherweise in JSON), die die Struktur der Daten beschreibt.

## 🔹 Einfach ausgedrückt

Avro = binäres JSON + Schema

Man kann sich Avro als eine effizientere, strukturierte Version von JSON vorstellen.
Anstatt wie JSON ausführliche Feldnamen und Werte zu senden,
überträgt Avro eine kleine binäre Nutzlast, die mithilfe des Schemas gelesen werden kann.

## 🔹 Beispiel

Hier ein kleines Avro-Schema:

```json
{
  "type": "record",
  "name": "CustomerEventRecord",
  "namespace": "de.sample.schulung.accounts.kafka",
  "fields": [
    { "name": "eventType", "type": "string" },
    { "name": "customerUuid", "type": "string" },
    {
      "name": "customer",
      "type": {
        "type": "record",
        "name": "CustomerRecord",
        "fields": [
          { "name": "name", "type": "string" },
          { "name": "birthdate", "type": "string" },
          { "name": "state", "type": "string" }
        ]
      }
    }
  ]
}
```

Dieses Schema beschreibt, welche Felder existieren, welche Typen sie haben und wie sie verschachtelt sind.

## 🔹 Wie Avro in Kafka funktioniert

Kafka-Nachrichten können mit Avro anstelle von JSON oder einfachen Strings kodiert werden.
Bei Verwendung von Avro mit einer Schema Registry (wie Apicurio oder Confluent Schema Registry):

1. Das Schema wird in der Registry gespeichert und versioniert.
2. Die an Kafka gesendete Nachricht enthält:
  - eine kleine Schema-ID, und
  - die binär kodierten Daten gemäß diesem Schema.

Consumer müssen das Schema also nicht vorab kennen — sie rufen es einfach per ID aus der Registry ab und dekodieren die Nachricht.

## 🔹 Warum Avro verwenden?

| Merkmal                     | Avro                                             | JSON               |
|-----------------------------|--------------------------------------------------|---------------------|
| Größe                       | Binär (sehr klein)                               | Text (ausführlich)  |
| Geschwindigkeit             | Schnelle (De)Serialisierung                      | Langsamer           |
| Schema-Evolution            | Eingebaut (rückwärts-/vorwärtskompatibel)        | Manuelle Validierung|
| Typsicherheit               | Stark typisiert                                  | Lose Typisierung    |
| Schema-Registry-Integration | Ja                                               | Optional            |

## ⚙️ Avro mit Kafka verwenden

### Quarkus

Details finden sich in [diesem Guide](https://quarkus.io/guides/kafka-schema-registry-avro).

### Spring Boot

Details finden sich in der [Confluent-Dokumentation](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html).
Es gibt auch eine Integration für [Spring Cloud Stream](https://www.baeldung.com/spring-cloud-stream-kafka-avro-confluent).
