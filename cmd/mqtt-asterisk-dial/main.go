package main

import (
	"flag"
	"log"
	config "mqtt-asterisk-dial/internal/config"
	"mqtt-asterisk-dial/internal/dial"

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
		})

	// Initialize MQTT client
	mqttClient := mqtt.NewClient(opts)

	for _, call := range cfg.Calls {
		log.Printf("Processing call: %s", call.Name)
		dialer, err := dial.NewDialer(mqttClient, cfg.CallFileDir, call)
		if err != nil {
			log.Fatalf("Error creating dialer: %v", err)
		}
		dialers = append(dialers, dialer)
	}

	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Wait until the app is interrupted
	select {}
}
