# mqtt-asterisk-dial

mqtt-asterisk-dial is a Go application that connects to an MQTT broker. It obereves one or more MQTT topics. When the values of such a topic has a defined
values then an Asterisk call file is written.

The content of the call files is rendered via a GO template. This template has 
access to values of MQTT topics.

# Configuration

The mqtt-asterisk-dial is configured via a configuration yaml file that
can be configured via the `-config` command line param. Default location is `./conf.yaml`).

You can find a sample configuration file [here](./conf.yaml).

## Placing calls via AMI

Instead of writing call files, mqtt-asterisk-dial can place calls via the
Asterisk Manager Interface (AMI, `Originate` action). This mode is enabled by
an `ami` section in the configuration; each call then needs an `originate`
block instead of a `template`. A sample configuration is [here](./conf-ami.yaml).

With AMI, mqtt-asterisk-dial no longer needs access to the Asterisk spool
directory and can run on a different host or container.

The client logs in with the MD5 challenge, keeps the session alive with
pings and reconnects with backoff. A call that is triggered while AMI is not
available waits up to two minutes for the session to come back.

Asterisk needs a manager user that may only originate calls, e.g. in
`manager.conf`:

```
[general]
enabled = yes
port = 5038
bindaddr = 0.0.0.0

[dialer]
secret = change-me
deny = 0.0.0.0/0.0.0.0
permit = 10.0.0.0/255.0.0.0
read = call
write = originate
```

`read = call` is needed for the `OriginateResponse` event, which is logged
for each call. Note that for a `Local` channel whose dialplan never answers
the channel itself (e.g. `Dial` with a `U()` subroutine that aborts the
bridge), Asterisk reports `Failure` with reason 0 even if the callee answered.

## Health endpoint

If `health_listen` is set, `GET /healthz` returns 200 when the MQTT
connection is open, all topics are subscribed since the last (re)connect and,
in AMI mode, the AMI session is logged in. Otherwise it returns 503 with the
reasons. Use it as a liveness probe.

## Usage in Docker

To use mqtt-asterisk-dial with Docker, you can utilize the provided Docker Compose configuration. You can find the sample Docker Compose file [here](./samples/docker/docker-compose.yaml).

## Project Structure

```
mqtt-asterisk-dial
├── cmd
│   └── mqtt-asterisk-dial
│       └── main.go        # Entry point of the application
├── internal
│   ├── config
│   │   └── config.go      # Configuration settings
│   ├── mqtt
│   │   └── client.go      # MQTT client management
│   └── dial
│       └── dial.go        # Dialing logic
├── go.mod                  # Module definition
└── README.md               # Project documentation
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