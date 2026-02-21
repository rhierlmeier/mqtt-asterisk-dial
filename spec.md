# Aufgabe: SIP-Call direkt (ohne Asterisk ausführen)

Ändere das Projekt so ab, sodass es keine Abhängigkeit von Asterisk mehr hat. 
Stattdessen soll der SIP-Call direkt an eine Fritzbox gesendet werden.

Der Inhalt des MQTT-Topics ist eine Nummer. In der conf.yaml soll unter dieser Nummer eine Audiodatei hinterlegt sein,
die im SIP-Call übermittelt wird.

Struktur der conf.yaml:
```yaml
numbers:
  - 089/123133
  - 089/123134

messages:
  - mqtt_topic: <name of the mqtt topic>
    mqtt_values:
        - value: <value of the mqtt topic>
          audioFile: <path to the audio file>
```         