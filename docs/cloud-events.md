# Cloud Events

CloudEvents ist ein [offener Standard](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md), der eine einheitliche Art beschreibt, Events zwischen verschiedenen Systemen, Services oder Cloud-Plattformen zu beschreiben und auszutauschen.

Kurz gesagt: Er definiert eine gemeinsame Sprache für Events, um sie portabel und interoperabel zu machen.

## Zweck

Viele Cloud-Services und Event-Systeme verwenden eigene Event-Formate. CloudEvents schafft ein Standardschema, sodass Events problemlos von verschiedenen Systemen konsumiert werden können. Dies ist nützlich für eventbasierte Architekturen, serverlose Anwendungen und Event-Streaming.

## Grundstruktur

Ein CloudEvent enthält typischerweise folgende Standardattribute:

| Attribut           | Beschreibung                                                    |
|--------------------|-----------------------------------------------------------------|
| `id`               | Eindeutiger Event-Bezeichner                                    |
| `source`           | Herkunft des Events (z.B. URL oder Service-Name)                |
| `type`             | Event-Typ (z.B. order.created)                                  |
| `specversion`      | CloudEvents-Spezifikationsversion                               |
| `time`             | Zeitstempel der Event-Erstellung                                |
| `datacontenttype`  | Content-Type der Nutzdaten (z.B. `application/json`)            |
| `data`             | Die eigentlichen Event-Nutzdaten (benutzerdefinierte Daten)     |

## Beispiel
```json
{
  "specversion": "1.0",
  "id": "1234-5678",
  "source": "/shop/orders",
  "type": "order.created",
  "time": "2025-10-28T12:34:56Z",
  "datacontenttype": "application/json",
  "data": {
    "orderId": "O1001",
    "customerId": "C001",
    "amount": 50.0
  }
}
```

## Vorteile

- Interoperabilität zwischen verschiedenen Event-Systemen (Kafka, AWS EventBridge, Azure Event Grid, etc.)
- Einheitliches Schema erleichtert Parsing, Validierung und Event-Routing
- Portabilität – dasselbe Event-Format kann über mehrere Clouds oder Plattformen hinweg verwendet werden
