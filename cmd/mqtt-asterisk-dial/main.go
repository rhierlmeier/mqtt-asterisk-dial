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

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientId).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password)

	// Initialize MQTT client
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	dialer, err := dial.NewDialer(mqttClient, cfg)
	if err != nil {
		log.Fatalf("Error creating dialer: %v", err)
	}

	if err = dialer.Start(); err != nil {
		log.Fatalf("Error starting dialer: %v", err)
	}

	// Wait until the app is interrupted
	select {}
}
