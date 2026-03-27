# JSON Schema & Schema Registry

> [!NOTE]
> Dieser Guide erklärt, wie JSON Schema mit Kafka verwendet wird,
> einschließlich Schema-Registrierung, Nachrichtenvalidierung und Consumer-Deserialisierung.


## Einführung

Kafka-Nachrichten können strukturierte JSON-Nutzdaten transportieren, die sich über die Zeit weiterentwickeln.
Schema-Registries gewährleisten Kompatibilität, Validierung und Governance von Nachrichtenformaten.

Sowohl Quarkus als auch Spring Boot unterstützen schema-basierte Serialisierung und Deserialisierung über die
- [Apicurio Registry](https://www.apicur.io/registry/) oder
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

## Beispiel-Domänenmodell

Nehmen wir an, wir möchten kundenbezogene Events publizieren und konsumieren.

```java
public record CustomerEventRecord(
  String eventType,
  UUID customerUuid,
  CustomerRecord customer
) {
}

public record CustomerRecord(
  String name,
  LocalDate birthdate,
  String state
) {
}
```

Eine beispielhafte JSON-Nachricht sieht so aus:

```json
{
  "eventType": "created",
  "customerUuid": "3b6f5c52-8b3f-45b1-9f36-12f56f8a67a1",
  "customer": {
    "name": "Tom Mayer",
    "birthdate": "2000-12-01",
    "state": "active"
  }
}
```

## JSON-Schema-Definition

Nachfolgend ein zugehöriges JSON-Schema für die Nachrichten-Nutzdaten, einschließlich Validierungsregeln:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "eventType": {
      "type": "string",
      "enum": [
        "created",
        "replaced",
        "deleted"
      ]
    },
    "customerUuid": {
      "type": "string",
      "format": "uuid"
    },
    "customer": {
      "type": "object",
      "properties": {
        "name": {
          "type": "string",
          "minLength": 3,
          "maxLength": 255
        },
        "birthdate": {
          "type": "string",
          "format": "date"
        },
        "state": {
          "type": "string",
          "enum": [
            "active",
            "locked",
            "disabled"
          ]
        }
      },
      "required": [
        "name",
        "birthdate",
        "state"
      ]
    }
  },
  "required": [
    "eventType",
    "customerUuid"
  ]
}
```

> [!NOTE]
> Man kann die Schema-Datei einfach im Producer-Projekt unter `src/main/resources` ablegen, wo sie
> zur Laufzeit beim Classpath-Scanning aufgelöst und automatisch in der Schema Registry registriert wird.

## Schema Registry einrichten

In diesem Beispiel verwenden wir die [Apicurio Registry](https://www.apicur.io/registry/), die mit den Confluent Schema Registry APIs kompatibel ist.

### Apicurio per Docker starten

```bash
docker run -it --rm -p 9080:8080 \
  -e REGISTRY_AUTH_ANONYMOUS_READ_ACCESS_ENABLED=true \
  -e REGISTRY_AUTH_ANONYMOUS_WRITE_ACCESS_ENABLED=true \
  quay.io/apicurio/apicurio-registry-mem:2.6.13.Final
```

Die Registry ist dann erreichbar unter:
👉 http://localhost:9080/apis/registry/v2/

> [!CAUTION]
> Es wird eine Version `>= 2.5.x` benötigt, da die API sonst einen anderen Endpunkt hat und nicht zu den Standardeinstellungen passt.
> Leider ist der `latest`-Tag derzeit einer älteren Version zugewiesen, daher ist hier Vorsicht geboten.

Anschließend kann das Schema mit einer Artefakt-ID hochgeladen werden, z.B. `customer-events`.

### Quarkus-Konfiguration

Die Quarkus-Anwendung wird für die Nutzung der Registry und JSON-Schema-Serialisierung konfiguriert. In diesem Beispiel
deaktivieren wir die [Quarkus Dev Services für die Apicurio Registry](https://quarkus.io/guides/apicurio-registry-dev-services),
da wir sie bereits extern betreiben.

```properties
# --- Schema Registry ---
quarkus.apicurio-registry.devservices.enabled=false

# --- Outgoing Channel ---
mp.messaging.outgoing.customers.value.serializer=de.sample.schulung.accounts.kafka.QuarkusJsonSchemaKafkaSerializer
# Schema für den Channel `movies` festlegen.
# Diese Property akzeptiert einen Namen oder Pfad und der Serializer sucht die Ressource im Classpath.
mp.messaging.outgoing.customers.apicurio.registry.url=http://localhost:9080/apis/registry/v2
mp.messaging.outgoing.customers.apicurio.registry.auto-register=true
mp.messaging.outgoing.customers.apicurio.registry.artifact.schema.location=customer-events.schema.json

# --- Incoming Channel ---
mp.messaging.incoming.customers.value.deserializer=de.sample.schulung.statistics.kafka.CustomerEventJsonSchemaKafkaDeserializer
mp.messaging.incoming.customers.apicurio.registry.deserializer.value.return-class=de.sample.schulung.statistics.kafka.CustomerEventRecord
mp.messaging.incoming.customers.apicurio.registry.url=http://localhost:8081/apis/registry/v2
mp.messaging.incoming.customers.apicurio.registry.auto-register=false
# mp.messaging.incoming.customers.apicurio.registry.group-id=
# mp.messaging.incoming.customers.apicurio.registry.artifact-id=
```

> [!NOTE]
> Die für die Deserialisierung verwendete Klasse wird in der Property `return-class` angegeben.
> Dies ist erforderlich, damit der Deserializer die Nachricht deserialisieren kann.
> Andernfalls liest der Deserializer sie aus dem Schema (Element `javaType`), was keine
> portable Lösung zu sein scheint.

### Spring-Boot-Konfiguration

Die Spring-Boot-Anwendung wird für die Nutzung der Registry und JSON-Schema-Serialisierung konfiguriert:

```yaml
spring:
  kafka:
    producer:
      value-serializer: io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer
      properties:
        apicurio.registry.url: http://localhost:9080/apis/registry/v2
        apicurio.registry.auto-register: true
        apicurio.registry.artifact.schema.location: customer-events.schema.json
    consumer:
      value-deserializer: io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer
      properties:
        apicurio.registry.url: http://localhost:9080/apis/registry/v2
        apicurio.registry.deserializer.value.return-class: de.sample.schulung.statistics.kafka.CustomerEventRecord
```

> [!NOTE]
> Die für die Deserialisierung verwendete Klasse wird in der Property `return-class` angegeben.
> Dies ist erforderlich, damit der Deserializer die Nachricht deserialisieren kann.
> Andernfalls liest der Deserializer sie aus dem Schema (Element `javaType`), was keine
> portable Lösung zu sein scheint.

### Eigene (De)Serializer

Leider gibt es eine mangelnde Integration der Apicurio-Kafka-(De)Serializer.
Der Jackson `ObjectMapper` wird von den (De)Serializern selbst erstellt, anstatt die vom Framework konfigurierte Instanz zu verwenden.
Dies führt zu Problemen, z.B. bei der Verwendung von Javas DateTime-API.
Daher müssen wir die Integration selbst übernehmen, indem wir eigene Klassen
für Serialisierung und Deserialisierung ableiten.

#### Quarkus

```java
public class QuarkusJsonSchemaKafkaSerializer<T>
  extends JsonSchemaKafkaSerializer<T> {

  public QuarkusJsonSchemaKafkaSerializer() {
  }

  public QuarkusJsonSchemaKafkaSerializer(RegistryClient client, ArtifactReferenceResolverStrategy<JsonSchema, T> artifactResolverStrategy, SchemaResolver<JsonSchema, T> schemaResolver) {
    super(client, artifactResolverStrategy, schemaResolver);
  }

  public QuarkusJsonSchemaKafkaSerializer(RegistryClient client) {
    super(client);
  }

  public QuarkusJsonSchemaKafkaSerializer(SchemaResolver<JsonSchema, T> schemaResolver) {
    super(schemaResolver);
  }

  public QuarkusJsonSchemaKafkaSerializer(RegistryClient client, Boolean validationEnabled) {
    super(client, validationEnabled);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    try (final var mapper = Arc.container().instance(ObjectMapper.class)) {
      this.setObjectMapper(
        mapper
          .orElse(
            new ObjectMapper()
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
              .registerModule(new JavaTimeModule())
          )
      );
    }
  }

  @Override
  public void setObjectMapper(ObjectMapper objectMapper) {
    if (null != objectMapper) {
      objectMapper.registerModule(new JavaTimeModule());
    }
    super.setObjectMapper(objectMapper);
  }

}
```

```java
public class QuarkusJsonSchemaKafkaDeserializer
  extends JsonSchemaKafkaDeserializer<CustomerEventRecord> {

  public QuarkusJsonSchemaKafkaDeserializer() {
  }

  public QuarkusJsonSchemaKafkaDeserializer(RegistryClient client, SchemaResolver<JsonSchema, CustomerEventRecord> schemaResolver) {
    super(client, schemaResolver);
  }

  public QuarkusJsonSchemaKafkaDeserializer(RegistryClient client) {
    super(client);
  }

  public QuarkusJsonSchemaKafkaDeserializer(SchemaResolver<JsonSchema, CustomerEventRecord> schemaResolver) {
    super(schemaResolver);
  }

  public QuarkusJsonSchemaKafkaDeserializer(RegistryClient client, Boolean validationEnabled) {
    super(client, validationEnabled);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    try (final var mapper = Arc.container().instance(ObjectMapper.class)) {
      this.setObjectMapper(
        mapper
          .orElse(
            new ObjectMapper()
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
              .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
              .registerModule(new JavaTimeModule())
          )
      );
    }
  }

  @Override
  public void setObjectMapper(ObjectMapper objectMapper) {
    if (null != objectMapper) {
      objectMapper.registerModule(new JavaTimeModule());
    }
    super.setObjectMapper(objectMapper);
  }

}
```

#### Spring Boot

```java
public class SpringJsonSchemaKafkaSerializer<T>
  extends JsonSchemaKafkaSerializer<T> {

  private final ObjectMapper objectMapper;

  public SpringJsonSchemaKafkaSerializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    this.setObjectMapper(objectMapper);
  }

}
```

```java
public class SpringJsonSchemaKafkaDeserializer<T>
  extends JsonSchemaKafkaDeserializer<T> {

  private final ObjectMapper objectMapper;

  public SpringJsonSchemaKafkaDeserializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    this.setObjectMapper(objectMapper);
  }

}
```
