# Trainer-Auflösung: Partitionierung & Partition Key

> **Nicht für Teilnehmer.** Diese Datei ist über `_config.yml` von der Veröffentlichung
> ausgeschlossen. Übung: [`issues/partitioning.md`](../issues/partitioning.md).

Es gibt selten *eine* richtige Antwort — bewertet wird die **Begründung**. Die folgenden
Tabellen zeigen die erwartete Argumentation.

## Begriffsdefinitionen (zum Abgleich)

Die Teilnehmer wiederholen diese Begriffe selbst (kein vorgegebener Text im Material). Erwartete
Kerninhalte:

| Begriff                          | Bedeutung                                                                                                              |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------|
| **Partition**                    | Ein Topic ist in mehrere Partitionen aufgeteilt. Jede Partition ist eine eigene, geordnete Sequenz von Nachrichten.   |
| **Partition Key**                | Optionaler Schlüssel einer Nachricht (zusätzlich zum eigentlichen Inhalt, dem *Value*).                               |
| **Default Partitioner**          | Standardregel: `Partition = hash(Key) % Anzahl Partitionen`. **Gleicher Key → immer dieselbe Partition.**             |
| **Reihenfolge-Garantie**         | Kafka garantiert die Reihenfolge **nur innerhalb einer Partition** — *nicht* über das ganze Topic hinweg.             |
| **Parallelität**                 | Pro Consumer Group kann jede Partition von **höchstens einem** Consumer gelesen werden → Partitionen = Obergrenze der Parallelität. |
| **Hot Partition / Datenschiefe** | Wenn ein Key-Wert sehr viel häufiger vorkommt als andere, landet überproportional viel Last auf einer Partition.      |

> Hinweis: **Round-Robin** ist bewusst *nicht* in der Begriffsliste — es ist genau das, was die
> Teilnehmer über die erste Reflexionsfrage (Verteilung ohne Key) selbst herausfinden sollen.

**Die zwei Konsequenzen jeder Key-Wahl** (das didaktische Gerüst — bewusst nicht vorgegeben):

1. **Reihenfolge:** Alle Nachrichten mit demselben Key landen in derselben Partition und werden
   damit **in der Sendereihenfolge** verarbeitet.
2. **Verteilung:** Damit Last und Parallelität gleichmäßig sind, sollten die Key-Werte **viele
   und gleich häufig** sein.

> 💡 Merksatz: Ein guter Key bringt zusammen, was zusammen gehören muss (Reihenfolge), und streut
> den Rest möglichst breit (Verteilung). Oft stehen diese beiden Ziele im Widerspruch.

## Szenario 1 — Kontobewegungen (Reihenfolge ist Pflicht)

| Key-Kandidat   | Reihenfolge                    | Verteilung                  | Hot-Partition | Parallelität | Geeignet? |
|----------------|--------------------------------|-----------------------------|---------------|--------------|-----------|
| `kontonummer`  | ✅ pro Konto streng geordnet   | ✅ viele Konten, breit       | gering        | gut          | **✅ ja**  |
| `buchungsId`   | ❌ jede Buchung eigene Partition| ✅ perfekt gestreut          | keins         | maximal      | ❌ nein    |
| kein Key       | ❌ Round-Robin, keine Ordnung  | ✅ gleichmäßig               | keins         | maximal      | ❌ nein    |

**Empfehlung: `kontonummer`.** Das ist der Lehrbuchfall „natürlicher Aggregat-Schlüssel": Alle
Buchungen eines Kontos landen in derselben Partition und werden dadurch in Reihenfolge
verarbeitet. `buchungsId` und „kein Key" maximieren zwar die Verteilung, zerstören aber die
fachlich zwingende Reihenfolge — disqualifiziert.

## Szenario 2 — Webshop mit Großkunden (Reihenfolge ↔ Verteilung im Konflikt)

| Key-Kandidat | Reihenfolge                  | Verteilung                          | Hot-Partition                  | Parallelität           | Geeignet? |
|--------------|------------------------------|-------------------------------------|--------------------------------|------------------------|-----------|
| `kundenId`   | ✅ pro Kunde geordnet        | ❌ stark schief (1 Kunde = 80 %)     | ⚠️ **massive Hot Partition**   | praktisch begrenzt     | Kompromiss|
| `bestellId`  | ❌ keine Ordnung pro Kunde   | ✅ perfekt gleichmäßig               | keins                          | maximal                | Kompromiss|

**Das ist die Kern-Übung: Es gibt keine saubere Lösung — der Zielkonflikt soll sichtbar werden.**

* `kundenId` erfüllt die fachliche Reihenfolge-Anforderung, erzeugt aber eine **Hot Partition**:
  Die Partition des Großkunden trägt 80 % der Last, ihr Consumer wird zum Flaschenhals, die
  übrigen Partitionen langweilen sich.
* `bestellId` verteilt perfekt, verliert aber die per-Kunde-Reihenfolge.

**Erwartete Reife-Antwort:** Den Konflikt benennen und gegen die Anforderung abwägen. Wenn die
Reihenfolge pro Kunde *wirklich* fachlich nötig ist → `kundenId` akzeptieren und die Hot
Partition operativ behandeln. Mögliche „dritte Wege", die starke Gruppen nennen können:
* **Zusammengesetzter Key** (`kundenId` + z.B. Bestellmonat/Region), um den Großkunden auf
  mehrere Partitionen zu splitten — nur sinnvoll, wenn nicht alle 80 % desselben Sub-Werts sind.
* **Eigenes Topic** für den Großkunden mit eigener Skalierung.
* Reihenfolge-Anforderung **hinterfragen**: Reicht Reihenfolge pro *Bestellung* statt pro
  *Kunde*? Dann genügt `bestellId`.

## Szenario 3 — Klick-Events / Analytics (kein Key ist richtig)

| Key-Kandidat | Reihenfolge        | Verteilung                          | Hot-Partition                | Parallelität        | Geeignet?     |
|--------------|--------------------|-------------------------------------|------------------------------|---------------------|---------------|
| kein Key     | (egal, nicht nötig)| ✅ Round-Robin, optimal             | keins                        | maximal             | **✅ ja**      |
| `userId`     | (unnötig)          | ✅ meist breit                       | ⚠️ je nach Power-Usern möglich| ok                  | ok, unnötig   |
| `seitenUrl`  | (unnötig)          | ❌ wenige Werte → wenige Partitionen | ⚠️ `/start` dominiert        | **stark begrenzt**  | ❌ nein        |

**Empfehlung: kein Key (Round-Robin).** Die Lernpointe: Wenn Reihenfolge *egal* ist und nur
Durchsatz zählt, ist „kein Key" die **beste** Wahl — gleichmäßigste Verteilung, maximale
Parallelität. `userId` schadet nicht, bringt aber keinen Nutzen (und kann bei Power-Usern leicht
schief werden). `seitenUrl` ist der **Antipattern-Köder**: wenige verschiedene Werte → nur wenige
Partitionen werden bespielt → Parallelität künstlich begrenzt + `/start` als Hot Partition.

> Falls später *pro Session/User* aggregiert werden soll, kann `sessionId`/`userId` doch sinnvoll
> werden — dann landet alles eines Users in einer Partition. Gute Überleitung zu Kafka Streams.

## Auflösung der Reflexionsfragen

* **Wie verteilt der Default Partitioner?**
  * **Mit Key:** `Partition = hash(Key) % Anzahl Partitionen`. Gleicher Key → **immer dieselbe**
    Partition. Die Last verteilt sich also nur so gleichmäßig, wie die Key-Werte gestreut sind
    (→ Hot-Partition-Gefahr bei schiefer Verteilung).
  * **Ohne Key (`null`):** Die Nachrichten werden **gleichmäßig reihum** auf alle Partitionen
    verteilt — das ist das **Round-Robin**-Verhalten. Maximale, gleichmäßige Verteilung, aber
    **keine** Zusammengehörigkeit/Reihenfolge mehr.
  * *(Genau das ist der Hebel hinter den Szenarien: „kein Key" = beste Verteilung, keine Ordnung;
    „Key" = Ordnung pro Key-Wert, Verteilung abhängig von der Key-Streuung.)*
* **Warum Reihenfolge nur pro Partition?** Eine topic-weite Reihenfolge würde bedeuten, dass es
  effektiv nur *eine* Sequenz gibt → nur **eine** Partition möglich → keine Parallelität, kein
  horizontaler Durchsatz. Die Partition ist die Einheit, in der Kafka Ordnung *und* Parallelität
  gegeneinander tauscht. Der Preis topic-weiter Ordnung wäre der Verlust der Skalierbarkeit.
* **Szenario-2-Kompromiss:** `kundenId` = Reihenfolge ja, aber Hot Partition; `bestellId` =
  perfekte Verteilung, aber keine per-Kunde-Reihenfolge. Dritter Weg: zusammengesetzter Key /
  eigenes Topic / Anforderung abschwächen (s.o.).
* **Wenige Key-Werte:** Es werden nur so viele Partitionen genutzt, wie es verschiedene Key-Werte
  gibt (genauer: `hash` mehrerer Werte kann auf dieselbe Partition fallen — also eher *weniger*).
  Die Parallelität ist damit hart gedeckelt, unabhängig davon, wie viele Partitionen das Topic
  hat oder wie viele Consumer in der Gruppe sind.
* **Zusammenhang Partition ↔ Consumer Group:** Pro Gruppe liest **höchstens ein** Consumer je
  Partition. Damit ist die **Partitionsanzahl die Obergrenze der Lastverteilung** — die offene
  Frage aus der Consumer-Group-Übung. Mehr Consumer als Partitionen → die überzähligen sind idle.
* **Partitionen nachträglich erhöhen:** Geht technisch (`kafka-topics --alter --partitions`),
  aber: Die Hash-Zuordnung `hash(Key) % n` ändert sich mit `n` → **derselbe Key landet künftig
  evtl. in einer anderen Partition** als seine bisherigen Nachrichten. Die per-Key-Reihenfolge
  über den Umstellungszeitpunkt hinweg ist damit gebrochen. Deshalb Partitionsanzahl möglichst
  von Anfang an großzügig planen.

## Didaktische Hinweise

* **Die drei Szenarien sind absichtlich ein Trio:** (1) Reihenfolge gewinnt klar, (2) echter
  Zielkonflikt ohne saubere Lösung, (3) „kein Key" ist die beste Lösung. Wenn nur Zeit für eines
  bleibt: Szenario 2 — daran lernt man am meisten.
* **Häufigster Denkfehler:** „Ein Key ist immer besser als kein Key." Szenario 3 widerlegt das.
* **Zweiter Denkfehler:** Verteilung wird vergessen, nur auf Reihenfolge geschaut (oder
  umgekehrt). Immer **beide** Spalten einfordern.
* **Brücke schlagen:** Die letzte Reflexionsfrage verbindet diese Übung wieder mit den Consumer
  Groups (Partitionsanzahl = Obergrenze der Parallelität) und schließt damit den Bogen.
* **Zeitbudget:** ca. 30-40 Min. (drei Szenarien) + 15 Min. Vorstellung/Diskussion.
