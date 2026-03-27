# Agents

Dieses Repository enthält Schulungsunterlagen (GitHub Pages, Jekyll) für die Schulung
**"Event-basierte Kommunikation mit Kafka"**.

## Kontext

Die Übungen beziehen sich auf zwei Vorlagen-Repositories, die jeweils in einer
**Quarkus**- und einer **Spring-Boot**-Variante vorliegen:

- [Quarkus-Kafka](https://github.com/ueberfuhr-trainings/quarkus-kafka)
- [Spring-Boot-Kafka](https://github.com/ueberfuhr-trainings/spring-boot-kafka)

Beide Repositories enthalten zwei Microservices:

| Service                        | Aufgabe                                              |
|--------------------------------|------------------------------------------------------|
| **account-service-provider**   | CRUD für Kunden, publiziert Events an Kafka          |
| **statistics-service-provider**| Konsumiert Kunden-Events, pflegt Kundenstatistiken   |

### Architektur der Services

Die Services folgen einer **Schichtenarchitektur** (angelehnt an Onion Architecture):

- **boundary** – REST-Schnittstelle (Spring Web / JAX-RS), DTOs, Mapper
- **domain** – Geschäftslogik (`CustomersService`), Domain-Modell, Events, Ports/Sinks
- **persistence** – JPA-Adapter (Spring Data JPA / Quarkus Panache)
- **kafka** – Kafka-Producer bzw. -Consumer
- **shared/interceptors** – Interceptor-basierte Event-Publikation

Zentrale Stelle ist der `CustomersService`. Über Interceptors (Spring AOP / Jakarta `@Interceptor`)
werden bei Methodenaufrufen Domain-Events gefeuert, die von der Kafka-Schicht konsumiert und
an ein Kafka-Topic gesendet werden (CDI Events / Spring Events).

### Branches und Musterlösungen

| Branch               | Inhalt                                                                 |
|----------------------|------------------------------------------------------------------------|
| `main`               | Vorlage für Teilnehmer (zum Klonen/Download)                          |
| `feature/producer`   | Musterlösung: Kafka-Producer im account-service-provider               |
| `feature/consumer`   | Musterlösung: Kafka-Consumer im statistics-service-provider            |

Zu jedem Feature-Branch existiert ein **Pull Request** als Musterlösung.

## Übungen formulieren

Übungen werden als Markdown-Dateien im Verzeichnis `issues/` abgelegt. Sie folgen dem
Template in `issues/issue_template.md` mit den Abschnitten: Lernziele, Definition of Done,
Arbeitsschritte, Selbstlernmaterial, Reflexionsfragen.

Übungen sollen **framework-unabhängig** formuliert werden und für beide Varianten
(Quarkus und Spring Boot) gelten. Framework-spezifische Hinweise können als Tipps
ergänzt werden.

## Technologie

- **Site-Generator:** Jekyll mit Theme `minima`
- **Sprache:** Deutsch
- **Layout:** `_layouts/default.html` (enthält Copy-Markdown-Button)
