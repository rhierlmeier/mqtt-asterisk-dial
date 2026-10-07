package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/ami"
	config "mqtt-asterisk-dial/internal/config"
	"mqtt-asterisk-dial/internal/dial"
	"mqtt-asterisk-dial/internal/heartbeat"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {

	cfg := config.Config{}

	flag.Usage = func() {
		log.Printf("Usage of %s:\n", "mqtt-asterisk-dial")
		flag.PrintDefaults()
	}
	confFile := flag.String("config", "./config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load configuration
	err := cfg.LoadFromFile(*confFile)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	if len(cfg.Calls) == 0 {
		log.Fatalf("No calls configured in the configuration file %s", *confFile)
	}

	var dialers []*dial.Dialer
	var hb *heartbeat.Heartbeat

	// Subscriptions are (re)established in the OnConnect handler: with a clean
	// session the broker forgets them on every reconnect, e.g. after a broker restart.
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientId).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetOnConnectHandler(func(client mqtt.Client) {
			log.Printf("Connected to MQTT broker %s", cfg.Broker)
			for _, dialer := range dialers {
				if err := dialer.Start(); err != nil {
					log.Printf("Error subscribing: %v", err)
				}
			}
			if hb != nil {
				if err := hb.Start(); err != nil {
					log.Printf("Error subscribing: %v", err)
				}
			}
		}).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("Connection to MQTT broker lost: %v", err)
			for _, dialer := range dialers {
				dialer.MarkUnsubscribed()
			}
			if hb != nil {
				hb.MarkUnsubscribed()
			}
		})

	// Initialize MQTT client
	mqttClient := mqtt.NewClient(opts)

	// In AMI mode calls are placed via Originate, otherwise via call files.
	var amiClient *ami.Client
	var originator dial.Originator
	if cfg.Ami != nil {
		amiClient = ami.New(ami.Config{
			Address:  net.JoinHostPort(cfg.Ami.Host, strconv.Itoa(cfg.Ami.Port)),
			Username: cfg.Ami.Username,
			Secret:   cfg.Ami.Secret,
		})
		originator = amiClient
		go amiClient.Run(context.Background())
	}

	for _, call := range cfg.Calls {
		log.Printf("Processing call: %s", call.Name)
		dialer, err := dial.NewDialer(mqttClient, cfg.CallFileDir, call, originator)
		if err != nil {
			log.Fatalf("Error creating dialer: %v", err)
		}
		dialers = append(dialers, dialer)
	}

	if cfg.Heartbeat != nil {
		hb = heartbeat.New(mqttClient, cfg.Heartbeat.Topic, time.Duration(cfg.Heartbeat.MaxAge)*time.Second)
	}

	if cfg.HealthListen != "" {
		go serveHealth(cfg.HealthListen, mqttClient, dialers, amiClient, hb)
	}

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Wait until the app is interrupted
	select {}
}

// serveHealth serves the health endpoints:
//   - /healthz (liveness): 200 if the MQTT connection is open, all topics are
//     subscribed and (in AMI mode) the AMI session is logged in, else 503.
//   - /readyz (readiness): like /healthz, and additionally the heartbeat (if
//     configured) is not stale. A missing heartbeat is not fixed by a restart,
//     so it only affects readiness.
func serveHealth(addr string, mqttClient mqtt.Client, dialers []*dial.Dialer, amiClient *ami.Client, hb *heartbeat.Heartbeat) {
	liveness := func() []string {
		var problems []string
		if !mqttClient.IsConnectionOpen() {
			problems = append(problems, "mqtt not connected")
		}
		for i, dialer := range dialers {
			if !dialer.Subscribed() {
				problems = append(problems, fmt.Sprintf("call %d not subscribed", i))
			}
		}
		if amiClient != nil && !amiClient.LoggedIn() {
			problems = append(problems, "ami not logged in")
		}
		return problems
	}
	respond := func(w http.ResponseWriter, problems []string) {
		if len(problems) > 0 {
			http.Error(w, strings.Join(problems, "\n"), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, liveness())
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		problems := liveness()
		if hb != nil {
			if problem := hb.Check(); problem != "" {
				problems = append(problems, problem)
			}
		}
		respond(w, problems)
	})
	log.Printf("Health endpoints listening on %s (/healthz, /readyz)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
