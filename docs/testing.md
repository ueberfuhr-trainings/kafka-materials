# Testen

Beim Testen einer Anwendung, die ein Kafka-Client ist, müssen verschiedene Konzepte berücksichtigt werden.

## Black-Box-Testing

Hierbei wird die Anwendung anhand ihres externen, beobachtbaren Verhaltens getestet. Sie verbindet sich mit einem laufenden Kafka-Broker
(einzeln oder als Cluster) und sendet bzw. empfängt Nachrichten.

### Voraussetzungen

In diesem Fall sind wir auf einen laufenden Kafka-Broker angewiesen, z.B.
 - als externe Ressource
 - als lokal laufender Container, z.B. unter Verwendung von
   - [Test Containers (Kafka-Modul)](https://java.testcontainers.org/modules/kafka/)
   - **Quarkus:** [Quarkus Dev Services for Kafka](https://quarkus.io/guides/kafka-dev-services)
   - **Spring Boot:** [Embedded Kafka (spring-kafka-test)](https://docs.spring.io/spring-kafka/reference/testing.html)

### Teststrategie

Für einen Consumer sendet der Test Nachrichten an ein Topic und überprüft, ob die Nachrichten korrekt empfangen und verarbeitet werden.
Für einen Producer löst der Test das Senden von Nachrichten an ein Topic aus und überprüft, ob die Nachrichten
mit dem korrekten Inhalt vom Topic konsumiert werden können.

[Citrus](https://citrusframework.org/) kann zur einfachen Interaktion mit dem Kafka-Topic verwendet werden. Zusätzlich:
- **Quarkus:** Die [Kafka Companion Library](https://quarkus.io/guides/kafka#testing-using-a-kafka-broker) stellt Test-Hilfsmittel bereit.
- **Spring Boot:** Die [Spring Kafka Test Utilities](https://docs.spring.io/spring-kafka/reference/testing.html) stellen Test-Hilfsmittel bereit.

### Möglichkeiten und Einschränkungen

Dies testet...
- ✅ ... die Verbindung zum Kafka-Broker mit den Konfigurationseigenschaften, inkl. Acknowledgement und Fehlerbehandlung.
- ✅ ... die (De)Serialisierung von Keys und Nutzdaten.
- ✅ ... die Schema-Validierung.

Dies testet NICHT...
- ❌ ... internen Code, z.B. Transaktionsmanagement
- ❌ ... einige Fehlerszenarien


## Testen ohne echten Kafka-Broker

### Quarkus: SmallRye In-Memory Connector

Um einen lokal laufenden Kafka-Broker zu vermeiden, kann der
[SmallRye In-Memory Connector](https://smallrye.io/smallrye-reactive-messaging/4.21.0/concepts/testing/) verwendet werden.
Details zur Verwendung in einem Quarkus-Test finden sich im
[Quarkus Guide](https://quarkus.io/guides/kafka#testing-without-a-broker).

#### Möglichkeiten und Einschränkungen

Dies testet...
- ✅ ... korrektes Senden von Nachrichten.
- ✅ ... korrekte Channel-Auswahl.
- ✅ ... korrekte Key/Value/Struktur.
- ✅ ... korrekte Nachrichten-Metadaten.

Dies testet NICHT...
- ❌ ... ob Jackson (oder Avro, Protobuf, etc.) die Keys und Nutzdaten korrekt (de)serialisiert.
- ❌ ... ob die Kafka-Connector- und Channel-Konfiguration funktioniert.
- ❌ ... ob Kafka die Nachricht tatsächlich akzeptiert.

### Spring Boot: Embedded Kafka

Um einen lokal laufenden Kafka-Broker zu vermeiden, kann der
[Embedded Kafka Broker](https://docs.spring.io/spring-kafka/reference/testing.html#embedded-kafka-annotation) verwendet werden.
Spring Boot stellt die Annotation `@EmbeddedKafka` bereit, um einen In-Memory-Kafka-Broker für Tests zu starten.

#### Möglichkeiten und Einschränkungen

Dies testet...
- ✅ ... korrektes Senden von Nachrichten.
- ✅ ... korrekte Channel-Auswahl.
- ✅ ... korrekte Key/Value/Struktur.
- ✅ ... ob Jackson (oder Avro, Protobuf, etc.) die Keys und Nutzdaten korrekt (de)serialisiert.

Dies testet NICHT...
- ❌ ... ob die Kafka-Konfiguration gegen einen echten Broker funktioniert.
- ❌ ... Performance-Charakteristiken unter Last.

## Mocking

Es können auch die Beans gemockt werden, die Kafka-Events senden und empfangen. Damit kann der Kafka-Connector
vollständig deaktiviert werden, allerdings hat dies die eingeschränktesten Testmöglichkeiten und ist anfällig für Wartbarkeitsprobleme.
