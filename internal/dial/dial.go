package dial

import (
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/config"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ghettovoice/gosip"
)

type Dialer struct {
	mqttClient mqtt.Client
	config     config.Config
	sipServer  gosip.Server
	mu         sync.Mutex
	vars       map[string]string // Key is the MQTT topic, value is the latest payload
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
		vars:       make(map[string]string),
	}, nil
}

func (d *Dialer) Start() error {
	// Subscribe to all variable topics to keep track of their values
	for _, msgCfg := range d.config.Messages {
		for _, v := range msgCfg.Variables {
			topic := v.MqttTopic
			d.mqttClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
				d.mu.Lock()
				d.vars[msg.Topic()] = string(msg.Payload())
				d.mu.Unlock()
				log.Printf("Updated variable for topic %s: %s", msg.Topic(), string(msg.Payload()))
			})
			log.Printf("Subscribed to variable topic %s", topic)
		}

		// Subscribe to main message trigger topics
		topic := msgCfg.MqttTopic
		msgCfgCopy := msgCfg // Capture for closure
		d.mqttClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			d.onMessageReceived(msgCfgCopy, string(msg.Payload()))
		})
		log.Printf("Subscribed to trigger topic %s", topic)
	}

	return nil
}

func (d *Dialer) onMessageReceived(msgCfg config.Message, payload string) {
	log.Printf("Received message on %s: %s", msgCfg.MqttTopic, payload)

	// If the payload is not something indicating a trigger (e.g., "1", "true", or any non-empty string),
	// this logic can be adjusted. Based on spec.md, it seems the presence of a number or trigger is key.
	// For now, any message on the topic triggers the call.

	audioFile := d.resolveAudioFile(msgCfg)

	for _, number := range d.config.Numbers {
		go d.makeSIPCall(number, audioFile)
	}
}

func (d *Dialer) resolveAudioFile(msgCfg config.Message) string {
	d.mu.Lock()
	defer d.mu.Unlock()

	resolved := msgCfg.AudioFile
	for _, v := range msgCfg.Variables {
		val, ok := d.vars[v.MqttTopic]
		if !ok {
			val = "unknown"
		}
		placeholder := fmt.Sprintf("${%s}", v.Name)
		resolved = strings.ReplaceAll(resolved, placeholder, val)
	}
	return resolved
}

func (d *Dialer) makeSIPCall(number string, audioFile string) {
	log.Printf("Starting SIP call to %s with audio %s", number, audioFile)

	// Since we cannot implement a full SIP/RTP stack here, we provide a structured placeholder
	// that describes the steps for a direct SIP call to a Fritzbox.
	// In a real implementation, you would use a library that supports RTP or call an external tool.

	sipUser := d.config.SIP.User
	sipPass := d.config.SIP.Password
	sipHost := d.config.SIP.Host

	log.Printf("SIP Call Details: User=%s, Pass=[REDACTED], Host=%s, Target=%s", sipUser, sipHost, number)
	_ = sipPass // Future use for authentication

	// 1. Create SIP Invite message
	// 2. Add SDP with audio capabilities (PCMA/PCMU)
	// 3. Handle Authentication (401 Unauthorized -> Send with Credentials)
	// 4. On 200 OK: Start RTP Stream with audioFile
	// 5. On completion or timeout: Send BYE
}
