# Aufgabe: SIP-Call direkt (ohne Asterisk ausführen)

Ändere das Projekt so ab, sodass es keine Abhängigkeit von Asterisk mehr hat. 
Stattdessen soll der SIP-Call direkt an eine Fritzbox gesendet werden.

Der Inhalt des MQTT-Topics ist eine Nummer. In der conf.yaml soll unter dieser Nummer eine Audiodatei hinterlegt sein,
die im SIP-Call übermittelt wird.

Das Problem kann mehrere Meldungen verwalten.
Eine Meldung kann mehrere Variablen haben.
Der Pfad zu der Audiodatei kann Variable nutzen.


Beispiel Konfiguration-Datei conf.yaml:
```yaml
numbers:
  - 089/123133
  - 089/123134

messages:
  - mqtt_topic: homie/hargassner/stoerung/active
    variables:
      - name: storeNr
        mqtt_topic: homie/hargassner/stoerung/nr
    audio_file: /home/pi/audio/${storNr}.wav
```         