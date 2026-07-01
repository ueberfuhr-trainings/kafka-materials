# Kafka-Konfigurationseigenschaften

## Quarkus (SmallRye Kafka Connector)

Einige Properties finden sich in den Beispielen des [Quarkus Kafka Guide](https://quarkus.io/guides/kafka).
In diesem Guide findet sich auch eine Liste der Properties für den SmallRye Kafka Connector.

- [Incoming-Channel-Konfiguration](https://quarkus.io/guides/kafka#incoming-channel-configuration-polling-from-kafka)
- [Outgoing-Channel-Konfiguration](https://quarkus.io/guides/kafka#outgoing-channel-configuration-writing-to-kafka)

Für Details lohnt sich ein Blick in den Quellcode folgender Klassen:

- `io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration`
- `io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgingConfiguration`


Zum Beispiel findet sich die Property `mp.messaging.incoming.<channel>.group.id` in der Klasse `KafkaConnectorIncomingConfiguration`:

```java
/**
 * Extract the incoming configuration for the {@code smallrye-kafka} connector.
*/
public class KafkaConnectorIncomingConfiguration extends KafkaConnectorCommonConfiguration {

  // ...

  /**
  * Gets the group.id value from the configuration.
  * Attribute Name: group.id
  * Description: A unique string that identifies the consumer group the application belongs to.
  * If not set, a unique, generated id is used
  * @return the group.id
  */
  public Optional<String> getGroupId() {
    return config.getOptionalValue("group.id", String.class);
  }

    // ...

}
```

## Spring Boot (Spring for Apache Kafka)

Properties finden sich in der [Spring Boot Kafka-Dokumentation](https://docs.spring.io/spring-boot/appendix/application-properties/index.html#appendix.application-properties.integration).
Einen umfassenden Guide bietet die [Spring for Apache Kafka-Referenz](https://docs.spring.io/spring-kafka/reference/).

### Gängige Producer-Properties

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
spring.kafka.producer.acks=all
```

### Gängige Consumer-Properties

```properties
spring.kafka.consumer.group-id=my-group
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer
spring.kafka.consumer.auto-offset-reset=earliest
```

Für Details lohnt sich ein Blick in den Quellcode folgender Klassen:

- `org.springframework.boot.autoconfigure.kafka.KafkaProperties`
- `org.apache.kafka.clients.producer.ProducerConfig`
- `org.apache.kafka.clients.consumer.ConsumerConfig`

Zum Beispiel findet sich die Property `group.id` in der Klasse `ConsumerConfig`:

```java
public class ConsumerConfig extends AbstractConfig {

  // ...

  /**
   * <code>group.id</code>
   */
  public static final String GROUP_ID_CONFIG = "group.id";

  /**
   * A unique string that identifies the consumer group this application belongs to.
   */
  private static final String GROUP_ID_DOC = "A unique string that identifies the consumer group...";

  // ...

}
```
