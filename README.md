# Kafka-Schulungsunterlagen

Dieses Repository enthält die Übungsunterlagen für die Schulung **"Event-basierte Kommunikation mit Kafka"**, bereitgestellt als [GitHub Pages](https://ueberfuhr-trainings.github.io/kafka-materials/) (Jekyll).

## Vorlagen-Repositories

Die Übungen beziehen sich auf zwei Vorlagen-Repositories:

| Variante    | Repository                                                              |
|-------------|-------------------------------------------------------------------------|
| Quarkus     | [quarkus-kafka](https://github.com/ueberfuhr-trainings/quarkus-kafka)  |
| Spring Boot | [spring-boot-kafka](https://github.com/ueberfuhr-trainings/spring-boot-kafka) |

Beide Repositories enthalten zwei Microservices:

- **account-service-provider** – CRUD-Operationen für Kunden, publiziert Events an Kafka
- **statistics-service-provider** – Konsumiert Kunden-Events und pflegt Kundenstatistiken

### Branches

| Branch             | Beschreibung                                                       |
|--------------------|--------------------------------------------------------------------|
| `main`             | Vorlage zum Klonen/Download für Teilnehmer                        |
| `feature/producer` | Musterlösung: Kafka-Producer (account-service-provider)            |
| `feature/consumer` | Musterlösung: Kafka-Consumer (statistics-service-provider)         |

Zu den Feature-Branches existieren jeweils **Pull Requests**, die als Musterlösung dienen.

### Architektur

Die Services verwenden eine Schichtenarchitektur (angelehnt an Onion Architecture):

```
boundary → domain → persistence
                  → kafka
                  → shared/interceptors
```

- **boundary** – REST-API (Spring Web bzw. JAX-RS), DTOs
- **domain** – Geschäftslogik (`CustomersService`), Domain-Modell, Events, Ports
- **persistence** – JPA-Adapter
- **kafka** – Kafka-Producer / -Consumer
- **shared/interceptors** – Interceptor-basierte Event-Publikation (Spring AOP / Jakarta Interceptors)

Der `CustomersService` ist die zentrale Stelle: Über Interceptors werden bei CRUD-Operationen Domain-Events gefeuert (CDI Events / Spring Events), die von der Kafka-Schicht konsumiert und an ein Topic gesendet werden.

## Struktur dieses Repositories

```
├── _config.yml          # Jekyll-Konfiguration
├── _layouts/
│   └── default.html     # Layout mit Copy-Markdown-Button
├── index.md             # Startseite (GitHub Pages)
└── issues/
    └── *.md             # Übungsblätter
```

## Lokale Vorschau

```bash
bundle install
bundle exec jekyll serve
```
