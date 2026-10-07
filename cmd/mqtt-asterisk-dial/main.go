package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/ami"
	config "mqtt-asterisk-dial/internal/config"
	"mqtt-asterisk-dial/internal/dial"
	"net"
	"net/http"
	"strconv"
	"strings"

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
		}).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("Connection to MQTT broker lost: %v", err)
			for _, dialer := range dialers {
				dialer.MarkUnsubscribed()
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

	if cfg.HealthListen != "" {
		go serveHealth(cfg.HealthListen, mqttClient, dialers, amiClient)
	}

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Wait until the app is interrupted
	select {}
}

// serveHealth serves /healthz: 200 if the MQTT connection is open, all topics
// are subscribed and (in AMI mode) the AMI session is logged in, else 503.
func serveHealth(addr string, mqttClient mqtt.Client, dialers []*dial.Dialer, amiClient *ami.Client) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
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
		if len(problems) > 0 {
			http.Error(w, strings.Join(problems, "\n"), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	log.Printf("Health endpoint listening on %s/healthz", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
