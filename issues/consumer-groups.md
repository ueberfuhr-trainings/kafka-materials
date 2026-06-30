---
layout: default
title: "Übung: Consumer Groups"
---

# Consumer Groups: Wer liest was?

Ein Producer schreibt Nachrichten in einen gemeinsamen Nachrichtenstrom — aber wer liest sie,
und wie oft wird eine Nachricht verarbeitet? In dieser Übung entwerft Ihr **ohne Code** und nur mit Stift, Papier
oder einem Whiteboard, wie mehrere Services über **Consumer Groups** auf denselben
Nachrichtenstrom zugreifen. Partitionen spielen hier bewusst noch keine Rolle — die kommen
in der nächsten Übung.

## 🎯 Lernziele

* Du kannst die Begriffe **Producer**, **Consumer**, **Consumer Group** und **Offset**
  in eigenen Worten erklären.
* Du verstehst den Unterschied zwischen **Lastverteilung** (eine Nachricht wird von genau einem
  Consumer einer Gruppe verarbeitet) und **Broadcast / Fan-out** (jede Gruppe erhält *alle*
  Nachrichten).
* Du kannst für ein gegebenes Szenario entscheiden, welche Consumer in **dieselbe** und welche
  in **unterschiedliche** Consumer Groups gehören.
* Du kannst erklären, was beim **Ausfall** eines Consumers und beim **späteren Hinzukommen**
  einer neuen Gruppe passiert.

## 📖 Begriffe

Bevor Du das Szenario löst: Definiere die folgenden Begriffe **für Dich selbst** in eigenen
Worten. Sie tauchen in der Aufgabe wieder auf — wer sie sauber fassen kann, löst die Aufgabe
fast von allein. Nutze bei Bedarf das Selbstlernmaterial weiter unten.

* Producer
* Consumer
* Consumer Group
* Group-ID
* Offset

## 🧩 Das Szenario

Ein Online-Shop veröffentlicht jede neue Bestellung als Nachricht im Nachrichtenstrom
**`bestellungen`**. Auf diese Bestellungen sollen mehrere fachliche Funktionen reagieren:

1. **Bestätigungs-Mail** — Für jede Bestellung soll der Kunde **genau eine** Bestätigungs-E-Mail
   erhalten.
2. **Rechnung** — Für jede Bestellung soll **genau eine** Rechnung erzeugt werden.
3. **Versand** — Für jede Bestellung soll das Lager **genau einen** Versandauftrag erhalten.

Zusätzlich gilt: Zur Spitzenlastzeit reicht **eine** Instanz des Mail-Versands nicht aus —
hier sollen **drei** Instanzen parallel arbeiten, ohne dass ein Kunde drei Mails bekommt.

## 🪜 Arbeitsschritte

Bearbeite die Aufgabe zunächst **für Dich allein**. Tausche Dich erst danach mit den anderen aus.

1. **Begriffe klären:** Vergewissere Dich, dass Du die Begriffe aus der Liste oben in eigenen
   Worten erklären könntest.
2. **Consumer und Groups festlegen:** Überlege für das Szenario: **Wie viele** Consumer brauchst
   Du, und **wie viele** Consumer Groups? Welcher Consumer gehört in welche Group? Halte Deine
   Entscheidung fest — die Form (Tabelle, Skizze, Stichworte) wählst Du selbst — und **begründe**
   sie.
3. **Nachrichtenfluss durchspielen:** Es kommen die Bestellungen `B1`, `B2`, `B3`, `B4` an.
   Spiele durch, welcher Consumer welche Bestellung verarbeitet. Erhält jede fachliche Funktion
   wirklich jede Bestellung genau einmal?
4. **Störfälle durchspielen:** Was passiert, wenn **eine der Mail-Instanzen abstürzt**, während
   `B3` ankommt? Was passiert, wenn **später** ein neues Reporting-Team eine eigene Auswertung
   anschließt, die **alle bisherigen und künftigen** Bestellungen sehen möchte?
5. **Austauschen:** Vergleicht Eure Lösungen in der Gruppe. Wo habt Ihr Euch unterschieden, und
   warum?

## ✅ Definition of Done

* [ ] Du hast festgelegt, wie viele Consumer und wie viele Consumer Groups es gibt, und die
  Zuordnung begründet.
* [ ] Du kannst sagen, welche Nachricht von welchem Consumer verarbeitet wird.
* [ ] Du hast die beiden Störfälle (Ausfall, späterer Beitritt) durchgespielt.
* [ ] Du hast die Reflexionsfragen beantwortet.

## 📚 Selbstlernmaterial

* [Apache Kafka — Consumer Groups](https://kafka.apache.org/documentation/#intro_consumers) —
  Offizielle Einführung in Consumer und Consumer Groups.
* [Conduktor: Kafka Consumer Groups](https://learn.conduktor.io/kafka/kafka-consumer-groups-and-consumer-offsets/) —
  Anschauliche Erklärung mit Diagrammen.

## 🤔 Reflexionsfragen

* Warum würde es ein Problem geben, wenn die drei Mail-Instanzen **unterschiedliche** Group-IDs
  hätten?
* Warum würde es ein Problem geben, wenn Mail, Rechnung und Versand **dieselbe** Group-ID hätten?
* Was bedeutet es für den **Offset**, wenn eine ganz neue Consumer Group startet? Sieht sie nur
  neue Nachrichten oder auch die, die *vor* ihrem Start gesendet wurden?
* Was passiert mit der Arbeit eines abgestürzten Consumers — geht eine Nachricht verloren, oder
  übernimmt ein anderer?

