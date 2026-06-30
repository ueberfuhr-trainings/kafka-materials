---
layout: default
title: "Übung: Partitionierung & Partition Key"
---

# Partitionierung: Welcher Key ist der richtige?

In der vorigen Übung habt Ihr gesehen, wie Consumer Groups die Arbeit teilen — aber *wie* genau
verteilt Kafka die Nachrichten? Die Antwort sind **Partitionen**, und welche Nachricht in welche
Partition wandert, entscheidet der **Partition Key**. Diese Wahl ist eine der wichtigsten
Design-Entscheidungen überhaupt: Sie bestimmt **Reihenfolge** und **Lastverteilung** zugleich.

In dieser Übung bewertet Ihr **ohne Code** für drei Szenarien jeweils mehrere Key-Kandidaten.
Ein passendes Nachrichten-**Schema** entwerfen wir hier *nicht* — es geht ausschließlich um die
Wahl des Schlüssels.

## 🎯 Lernziele

* Du kannst erklären, was eine **Partition** ist und wie der **Partition Key** die Partition
  einer Nachricht bestimmt.
* Du verstehst, dass die **Reihenfolge** von Nachrichten nur **innerhalb einer Partition**
  garantiert ist.
* Du kannst den Zielkonflikt zwischen **Reihenfolge-Garantie** und **gleichmäßiger
  Lastverteilung** an konkreten Beispielen erklären.
* Du erkennst das Risiko einer **Hot Partition** (Datenschiefe) und kannst eine Key-Wahl
  daraufhin bewerten.
* Du kannst begründen, wann **kein** Key (Round-Robin) die richtige Wahl ist.

## 📖 Begriffe

Wiederhole die folgenden Begriffe **für Dich selbst** in eigenen Worten, bevor Du die Szenarien
bearbeitest. Sie sind das Handwerkszeug, um eine Key-Wahl zu bewerten. Nutze bei Bedarf das
Selbstlernmaterial weiter unten.

* Partition
* Partition Key
* Default Partitioner
* Reihenfolge-Garantie
* Parallelität
* Hot Partition / Datenschiefe

## 🪜 Arbeitsschritte (für jedes der drei Szenarien)

1. Lest das Szenario und die fachlichen Anforderungen.
2. Bewertet **jeden** Key-Kandidaten anhand von vier Kriterien:
   * **Reihenfolge:** Bleibt zusammen, was zusammen geordnet sein muss?
   * **Verteilung:** Verteilt sich die Last gleichmäßig auf die Partitionen?
   * **Hot-Partition-Risiko:** Gibt es Key-Werte, die viel häufiger vorkommen?
   * **Parallelität:** Können genügend Consumer gleichzeitig arbeiten?
3. Entscheidet Euch für **einen** Key (oder bewusst für *keinen*) und **begründet** die Wahl.
4. Haltet fest, welchen **Kompromiss** Ihr eingeht — die perfekte Lösung gibt es selten.

Nutzt pro Szenario diese Bewertungstabelle:

| Key-Kandidat | Reihenfolge | Verteilung | Hot-Partition-Risiko | Parallelität | Geeignet? |
|--------------|-------------|------------|----------------------|--------------|-----------|
|              |             |            |                      |              |           |

---

## 🧩 Szenario 1 — Kontobewegungen einer Bank

Eine Bank schreibt jede **Buchung** (Ein- und Auszahlungen, Überweisungen) als Nachricht in das
Topic **`kontobewegungen`**. Wichtig: Für ein einzelnes Konto müssen die Buchungen **streng in
der richtigen Reihenfolge** verarbeitet werden — sonst könnte ein Konto zwischenzeitlich ins
Minus rutschen oder eine Sperre falsch berechnet werden.

**Key-Kandidaten:**
* `kontonummer`
* `buchungsId` (eine eindeutige ID je Buchung)
* *kein Key* (Round-Robin)

---

## 🧩 Szenario 2 — Bestellungen eines Webshops mit einem Großkunden

Ein Webshop schreibt Bestell-Events in das Topic **`bestellungen`**. Pro Kunde sollen die Events
in Reihenfolge bleiben (z.B. *Bestellung angelegt* → *bezahlt* → *storniert*). Besonderheit:
**Ein einziger Großkunde** (ein Marktplatz-Partner) verursacht rund **80 %** aller Bestellungen,
die übrigen Tausenden Kunden teilen sich den Rest.

**Key-Kandidaten:**
* `kundenId`
* `bestellId` (eine eindeutige ID je Bestellung)

---

## 🧩 Szenario 3 — Klick-Events einer Website (Analytics)

Eine Website schreibt jeden Seitenaufruf / Klick als Event in das Topic **`klicks`**. Es geht
um **sehr hohe Datenmengen**, die später nur **statistisch aggregiert** ausgewertet werden
(z.B. „Aufrufe pro Stunde"). Die **Reihenfolge einzelner Klicks** spielt für die Auswertung
**keine Rolle**. Wichtig ist maximaler **Durchsatz** und gleichmäßige Auslastung vieler Consumer.

**Key-Kandidaten:**
* `userId`
* *kein Key* (Round-Robin)
* `seitenUrl` (z.B. `/start`, `/produkte`, `/warenkorb` …)

---

## ✅ Definition of Done

* [ ] Für **alle drei** Szenarien ist eine Bewertungstabelle ausgefüllt.
* [ ] Für jedes Szenario habt Ihr Euch für einen Key (oder bewusst keinen) entschieden und die
  Entscheidung **begründet**.
* [ ] Ihr könnt für mindestens ein Szenario den **Zielkonflikt** zwischen Reihenfolge und
  Verteilung benennen.
* [ ] Ihr habt die Reflexionsfragen beantwortet.

## 📚 Selbstlernmaterial

* [Apache Kafka — Topics & Partitions](https://kafka.apache.org/43/getting-started/introduction/#main-concepts-and-terminology) —
  Offizielle Einführung in Topics und Partitionen.
* [Conduktor: Partitions & Keys](https://learn.conduktor.io/kafka/kafka-topics-internals-segments-and-indexes/) —
  Wie Keys auf Partitionen abgebildet werden.
* [Confluent: Choosing the number of partitions](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/) —
  Hintergrund zu Partitionsanzahl und Durchsatz.
* [Hot Partitions (Datenschiefe)](../docs/hot-partitions.html) —
  Wie eine Hot Partition entsteht und welche Strategien sie auflösen (relevant für Szenario 2).

## 🤔 Reflexionsfragen

* Wie verteilt der **Default Partitioner** eine Nachricht auf die Partitionen, wenn ein **Key**
  verwendet wird — und wie, wenn **kein** Key verwendet wird?
* Warum garantiert Kafka die Reihenfolge **nur innerhalb einer Partition** und nicht über das
  ganze Topic? Was wäre der Preis einer topic-weiten Reihenfolge?
* In Szenario 2: Welchen Kompromiss geht Ihr mit `kundenId` ein, welchen mit `bestellId`?
  Gibt es eine dritte Möglichkeit?
* Was passiert mit der Parallelität, wenn ein Key nur **wenige verschiedene Werte** annehmen kann
  (z.B. `seitenUrl` mit fünf Seiten)?
* Wie hängen **Partitionen** mit der **Consumer Group** aus der vorigen Übung zusammen — wo liegt
  die Obergrenze der Lastverteilung?
* Was wäre nötig, um die Partitionsanzahl eines bestehenden Topics nachträglich zu erhöhen — und
  welches Problem entsteht dabei für die Reihenfolge bereits verteilter Keys?
