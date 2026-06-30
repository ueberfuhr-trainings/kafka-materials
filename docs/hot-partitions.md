# Hot Partitions (Datenschiefe)

> [!NOTE]
> Dieses Dokument erklärt, wie eine **Hot Partition** entsteht, warum sie ein Problem ist und
> welche Strategien es gibt, sie zu vermeiden oder aufzulösen.

## Einführung

Ein Kafka-Topic ist in mehrere **Partitionen** aufgeteilt. Welche Partition eine Nachricht
erhält, bestimmt standardmäßig der **Default Partitioner**:

- **Mit Key:** `Partition = hash(Key) % Anzahl Partitionen`. Derselbe Key landet damit **immer**
  in derselben Partition.
- **Ohne Key** (`null`): Die Nachrichten werden **reihum** gleichmäßig auf alle Partitionen
  verteilt (Round-Robin).

Eine **Hot Partition** entsteht, wenn ein einzelner Key-Wert (oder wenige) deutlich häufiger
vorkommt als alle anderen. Weil dieser Key immer dieselbe Partition trifft, trägt diese eine
Partition überproportional viel Last — man spricht von **Datenschiefe** (engl. *data skew*).

### Beispiel

Ein Webshop schreibt Bestell-Events mit dem Key `kundenId`. Ein einziger Großkunde verursacht
80 % aller Bestellungen. Folge: Die Partition dieses Kunden erhält 80 % aller Nachrichten,
während sich die übrigen Partitionen langweilen.

## Warum ist das ein Problem?

| Auswirkung              | Erläuterung                                                                                          |
|-------------------------|------------------------------------------------------------------------------------------------------|
| **Flaschenhals**        | Pro Consumer Group liest **höchstens ein** Consumer eine Partition. Die heiße Partition wird von genau einem Consumer verarbeitet — die übrigen Consumer helfen ihr nicht. |
| **Ungenutzte Parallelität** | Mehr Partitionen und mehr Consumer bringen nichts, solange die Last auf einer Partition konzentriert bleibt. |
| **Lag & Latenz**        | Die heiße Partition baut Rückstand (*Consumer Lag*) auf, ihre Nachrichten werden spät verarbeitet.   |
| **Ungleiche Auslastung**| Broker-seitig wachsen Speicher und I/O der heißen Partition ungleichmäßig.                            |

## Strategien zur Lösung

Es gibt keine Universallösung. Die richtige Wahl hängt vor allem davon ab, **ob die
Reihenfolge-Garantie pro Key wirklich benötigt wird**. Die folgenden Strategien sind grob nach
Aufwand und Eingriffstiefe sortiert.

### 1. Reihenfolge-Anforderung hinterfragen

Häufig ist die per-Key-Reihenfolge gar nicht so zwingend wie zunächst angenommen — und das ist
meist die **billigste** Lösung, weil sie nur Nachdenken kostet, keine Infrastruktur.

- Reicht Reihenfolge pro *Bestellung* statt pro *Kunde*? → `bestellId` als Key, perfekte Verteilung.
- Ist die Reihenfolge ganz egal (z.B. reine Analytics)? → **kein** Key, Round-Robin.

### 2. Key feiner schneiden (zusammengesetzter Key)

Den heißen Key mit einem zweiten Merkmal kombinieren, sodass er sich auf mehrere Partitionen
aufteilt:

- `kundenId` → `kundenId + region` / `kundenId + produktkategorie` / `kundenId + monat`
- **Voraussetzung:** Reihenfolge wird nur noch *innerhalb* der feineren Einheit benötigt, nicht
  mehr über den gesamten Großkunden hinweg.
- **Falle:** Bringt nichts, wenn der Großkunde ohnehin nur *einen* Wert des Zusatzmerkmals hat
  (z.B. nur eine Region) — dann bleibt die Partition heiß.

### 3. Eigenes Topic für den Heavy-Hitter

Den Großkunden auf ein separates Topic mit eigener Skalierung (und ggf. eigenen Consumern)
routen.

- **Vorteil:** Die vielen kleinen, normal verteilten Keys bleiben in einem gleichmäßig
  ausgelasteten Topic ungestört.
- **Nachteil:** Zusätzliche Komplexität; der Heavy-Hitter muss vorab bekannt oder erkennbar sein.

### 4. Salting (Notlösung)

Dem Key künstlich Streuung verpassen, z.B. `kundenId + zufallszahl(0..k)`. Das verteilt den
heißen Key auf `k` Partitionen.

- **Preis:** Die Reihenfolge geht verloren, und Consumer müssen die `k` Teilströme beim Lesen
  ggf. wieder zusammenführen (z.B. per Aggregation).
- Nur einsetzen, wenn die Strategien 1–3 nicht passen.

### Antipattern: einfach mehr Partitionen

Mehr Partitionen lösen eine Hot Partition **nicht**, solange der heiße Key derselbe bleibt:
`hash(Key) % n` schickt denselben Key weiterhin auf genau **eine** Partition. Mehr Partitionen
helfen gegen *generelle* Last, nicht gegen Schiefe durch einen einzelnen Key-Wert.

> [!WARNING]
> Die Partitionsanzahl nachträglich zu erhöhen ändert zudem die Zuordnung `hash(Key) % n` für
> **alle** Keys. Derselbe Key kann danach in einer anderen Partition landen als seine bisherigen
> Nachrichten — die per-Key-Reihenfolge über den Umstellungszeitpunkt hinweg ist damit gebrochen.

## Best Practices

1. **Verteilung der Key-Werte vorab abschätzen:** Gibt es einzelne Werte, die einen Großteil der
   Nachrichten ausmachen? Genau diese erzeugen Hot Partitions.
2. **Zuerst die Reihenfolge-Anforderung prüfen:** Oft führt schon die Frage *„Brauche ich die
   Reihenfolge pro diesem Key überhaupt?"* zu einer einfachen Lösung (Strategie 1 oder 2).
3. **Salting ist die Ausnahme, nicht der Standard:** Es opfert die Reihenfolge und verlagert
   Komplexität auf die Consumer.
4. **Consumer Lag pro Partition überwachen:** Eine dauerhaft zurückhängende Partition ist das
   typische Frühwarnzeichen für Datenschiefe.

## Weiterführende Themen

- [Konfiguration (Properties)](properties.html) — relevante Producer-Einstellungen
- [Acknowledgements (ACKs)](acks.html) — Zusammenspiel mit Zuverlässigkeit
- [Kafka Streams](kafka-streams.html) — Re-Partitionierung und Aggregation über Keys
