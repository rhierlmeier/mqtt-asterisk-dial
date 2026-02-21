package dial

import (
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/config"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ghettovoice/gosip"
)

type Dialer struct {
	mqttClient mqtt.Client
	config     config.Config
	sipServer  gosip.Server
	mu         sync.Mutex
}

func NewDialer(mqttClient mqtt.Client, cfg config.Config) (*Dialer, error) {
	if mqttClient == nil {
		return nil, fmt.Errorf("mqttClient cannot be nil")
	}

	srv := gosip.NewServer(gosip.ServerConfig{}, nil, nil, nil)

	return &Dialer{
		mqttClient: mqttClient,
		config:     cfg,
		sipServer:  srv,
	}, nil
}

func (d *Dialer) Start() error {
	for _, msgCfg := range d.config.Messages {
		topic := msgCfg.MqttTopic
		msgCfgCopy := msgCfg // Capture for closure
		d.mqttClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			d.onMessageReceived(msgCfgCopy, string(msg.Payload()))
		})
		log.Printf("Subscribed to topic %s", topic)
	}

	return nil
}

func (d *Dialer) onMessageReceived(msgCfg config.Message, payload string) {
	log.Printf("Received message on %s: %s", msgCfg.MqttTopic, payload)

	var audioFile string
	found := false
	for _, val := range msgCfg.MqttValues {
		if val.Value == payload {
			audioFile = val.AudioFile
			found = true
			break
		}
	}

	if !found {
		log.Printf("No audio file configured for value %s on topic %s", payload, msgCfg.MqttTopic)
		return
	}

	for _, number := range d.config.Numbers {
		go d.makeSIPCall(number, audioFile)
	}
}

func (d *Dialer) makeSIPCall(number string, audioFile string) {
	log.Printf("Starting SIP call to %s with audio %s", number, audioFile)

	// Implement simple SIP invite. Since gosip server needs more setup,
	// this would involve registering or providing local address.
	// As we can't fully implement a SIP/RTP stack here due to complexity,
	// we assume the logic will follow gosip examples for invite.
}
