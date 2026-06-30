# Trainer-Auflösung: Consumer Groups

> **Nicht für Teilnehmer.** Diese Datei ist über `_config.yml` von der Veröffentlichung
> ausgeschlossen. Übung: [`issues/consumer-groups.md`](../issues/consumer-groups.md).

## Begriffsdefinitionen (zum Abgleich)

Die Teilnehmer definieren diese Begriffe selbst. Erwartete Kerninhalte:

> Hinweis: Der Begriff **Topic** ist an dieser Stelle des Kurses noch nicht eingeführt und
> bewusst aus dem Teilnehmer-Material entfernt; im Szenario heißt er „Nachrichtenstrom".

| Begriff            | Bedeutung                                                                                  |
|--------------------|--------------------------------------------------------------------------------------------|
| **Producer**       | Komponente, die Nachrichten in den Nachrichtenstrom schreibt.                              |
| **Consumer**       | Eine laufende Anwendung(s-Instanz), die Nachrichten aus dem Nachrichtenstrom liest.        |
| **Consumer Group** | Eine Menge von Consumern, die über eine gemeinsame **Group-ID** zusammengefasst sind.      |
| **Group-ID**       | Frei wählbarer Name, der eine Consumer Group identifiziert.                                 |
| **Offset**         | Lesezeiger einer Gruppe: bis zu welcher Nachricht im Topic die Gruppe bereits gelesen hat. |

**Die zentrale Regel** (die das Szenario erfahrbar macht — bewusst *nicht* im Teilnehmer-Material):

* Innerhalb **einer** Consumer Group wird jede Nachricht von **genau einem** Consumer verarbeitet
  → **Lastverteilung**.
* **Verschiedene** Consumer Groups erhalten **jeweils alle** Nachrichten unabhängig voneinander
  → **Broadcast / Fan-out**.

> 💡 Merksatz: *Gleiche Group-ID = Arbeit teilen. Unterschiedliche Group-ID = jeder bekommt alles.*

## Erwartetes Ergebnis (Gruppenzuordnung)

Die Kernidee: **Jede fachliche Funktion ist eine eigene Consumer Group** (damit jede *alle*
Bestellungen sieht). **Skalierung innerhalb einer Funktion** geschieht durch mehrere Instanzen
in **derselben** Group.

| Consumer       | Group-ID          | Begründung                                                        |
|----------------|-------------------|-------------------------------------------------------------------|
| Mail-Instanz 1 | `mail-service`    | Drei Instanzen teilen sich die Arbeit → genau eine Mail je Order. |
| Mail-Instanz 2 | `mail-service`    | dito                                                              |
| Mail-Instanz 3 | `mail-service`    | dito                                                              |
| Rechnung       | `rechnung-service`| Eigene Gruppe → erhält *alle* Bestellungen, unabhängig von Mail.  |
| Versand        | `versand-service` | Eigene Gruppe → erhält *alle* Bestellungen, unabhängig von Mail.  |

Ergebnis: Jede Bestellung löst **genau eine** Mail, **genau eine** Rechnung und **genau einen**
Versandauftrag aus. Drei Gruppen insgesamt.

### Nachrichtenfluss (Schritt 4)

Für `B1..B4` bekommen `rechnung-service` und `versand-service` **jeweils alle vier**. Innerhalb
`mail-service` werden die vier auf die drei Instanzen verteilt (z.B. B1→I1, B2→I2, B3→I3, B4→I1).
Welche Instanz welche Bestellung bekommt, ist nicht vorhersehbar und auch egal — Hauptsache jede
Bestellung genau einmal innerhalb der Gruppe.

### Störfälle (Schritt 5)

* **Mail-Instanz 2 stürzt ab:** Kafka bemerkt den Ausfall und verteilt die Arbeit auf die
  verbleibenden Instanzen der Gruppe um (**Rebalancing**). Keine Bestellung geht verloren; die
  von I2 noch nicht bestätigte Nachricht wird von einer anderen Instanz übernommen. (Hier ggf.
  schon kurz „mindestens-einmal"-Semantik erwähnen — vertieft in der Übung zu Delivery-Semantik.)
* **Neues Reporting-Team:** Es bekommt eine **neue Group-ID** (z.B. `reporting`). Ob es *nur
  neue* oder *auch alte* Bestellungen sieht, hängt von `auto.offset.reset` ab: `earliest` =
  alle noch im Topic vorhandenen Nachrichten von vorne, `latest` = nur ab jetzt. Hier ist das
  die zentrale Lernpointe für den Offset-Begriff.

## Auflösung der Reflexionsfragen

* **Drei Mail-Instanzen mit *unterschiedlichen* Group-IDs?** Dann wäre jede Instanz eine eigene
  Gruppe → jede bekäme *alle* Bestellungen → der Kunde erhielte **drei** Mails. Falsch.
* **Mail, Rechnung, Versand mit *derselbe* Group-ID?** Dann teilen sie sich die Bestellungen
  untereinander auf → eine Bestellung würde z.B. *nur* die Rechnung auslösen, aber keine Mail
  und keinen Versand. Die Funktionen würden sich gegenseitig Nachrichten „wegnehmen". Falsch.
* **Offset einer neuen Gruppe:** Eine neue Group-ID hat noch keinen gespeicherten Offset →
  Verhalten über `auto.offset.reset` (`earliest` vs. `latest`). Wichtig: Nachrichten sind nicht
  „weg", nachdem sie gelesen wurden — sie bleiben gemäß **Retention** im Topic; jede Gruppe hat
  ihren eigenen Lesezeiger.
* **Abgestürzter Consumer:** Keine Nachricht geht verloren; nach Rebalancing übernimmt ein
  anderer Consumer derselben Gruppe. (Genau-einmal vs. mindestens-einmal ist ein eigenes Thema.)
* **Grenze der Lastverteilung:** Nein, nicht beliebig. Mehr Instanzen helfen nur bis zur Anzahl
  der **Partitionen** — überzählige Consumer einer Gruppe bleiben idle. **Das ist die geplante
  Brücke zur Partitionierungs-Übung.**

## Didaktische Hinweise

* **Partitionen bewusst weglassen.** Auf dieser Stufe ist „Kafka verteilt die Nachrichten unter
  den Consumern der Gruppe" eine zulässige Vereinfachung. Die exakte Wahrheit (Verteilung läuft
  über Partitionen, max. ein Consumer pro Partition je Gruppe) kommt in der nächsten Übung. Wenn
  Teilnehmer früh nach dem „Wie" der Verteilung fragen: auf die Folgeübung vertrösten.
* **Typischer Fehler:** Teilnehmer setzen reflexhaft alle Consumer in eine Gruppe („eine App =
  eine Gruppe") oder geben jeder Instanz eine eigene Gruppe. Beide Fehler sind didaktisch
  wertvoll — am Diagramm sofort sichtbar machen (zu viele/zu wenige Nachrichten).
* **Zeitbudget:** ca. 20-30 Min. Gruppenarbeit + 10 Min. Vorstellung.
* **Übergang:** Mit der letzten Reflexionsfrage direkt in die Partitionierungs-Übung überleiten.
