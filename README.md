# mqtt-sip-dial

`mqtt-sip-dial` ist eine Go-Anwendung, die sich mit einem MQTT-Broker verbindet und auf definierte Topics hört. Wenn eine Nachricht auf einem konfigurierten Trigger-Topic empfangen wird, initiiert die Anwendung einen direkten SIP-Anruf (z. B. an eine Fritzbox) und spielt eine konfigurierte Audiodatei ab.

Die Pfade zu den Audiodateien können Variablen enthalten, deren Werte aus zusätzlichen MQTT-Topics bezogen werden.

# Konfiguration

Die Anwendung wird über eine YAML-Datei konfiguriert, die über den Parameter `-config` angegeben werden kann (Standard: `./conf.yaml`).

Eine Beispielkonfiguration findest du [hier](./conf.yaml).

## SIP-Integration

Im Gegensatz zur früheren Version benötigt diese Anwendung kein Asterisk mehr. Der Anruf wird direkt per SIP an den konfigurierten Host (z. B. eine Fritzbox) gesendet.

## Projektstruktur

```
mqtt-sip-dial
├── cmd
│   └── mqtt-asterisk-dial
│       └── main.go        # Einstiegspunkt der Anwendung
├── internal
│   ├── config
│   │   └── config.go      # Konfigurationseinstellungen
│   └── dial
│       └── dial.go        # Dialing-Logik und SIP-Handling
├── go.mod                  # Modul-Definition
└── README.md               # Projektdokumentation
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd mqtt-asterisk-dial
   ```

2. **Install dependencies:**
   ```
   go mod tidy
   ```

3. **Configure the application:**
   Update the configuration settings in `internal/config/config.go` or set environment variables as needed.

## Usage

To run the application, execute the following command:

```
go run cmd/mqtt-asterisk-dial/main.go
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.