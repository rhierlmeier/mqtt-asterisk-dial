// Package heartbeat watches a topic that an external job publishes to
// periodically. A heartbeat that stops arriving shows that messages no longer
// reach the dialer, even though the MQTT connection looks healthy.
package heartbeat

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Heartbeat struct {
	mqttClient mqtt.Client
	topic      string
	maxAge     time.Duration
	now        func() time.Time

	// last is the time of the last heartbeat in Unix nanoseconds. It starts
	// at the creation time, so a fresh start has maxAge to receive the first one.
	last       atomic.Int64
	subscribed atomic.Bool
}

func New(mqttClient mqtt.Client, topic string, maxAge time.Duration) *Heartbeat {
	h := &Heartbeat{mqttClient: mqttClient, topic: topic, maxAge: maxAge, now: time.Now}
	h.last.Store(h.now().UnixNano())
	return h
}

// Start subscribes to the heartbeat topic. It must be called after every
// (re)connect, like Dialer.Start.
func (h *Heartbeat) Start() error {
	h.subscribed.Store(false)
	token := h.mqttClient.Subscribe(h.topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		h.received()
	})
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("heartbeat: could not subscribe to topic %s: %w", h.topic, token.Error())
	}
	log.Printf("Heartbeat: Subscribed to topic %s (max age %s)", h.topic, h.maxAge)
	h.subscribed.Store(true)
	return nil
}

// MarkUnsubscribed must be called when the MQTT connection is lost.
func (h *Heartbeat) MarkUnsubscribed() {
	h.subscribed.Store(false)
}

func (h *Heartbeat) received() {
	h.last.Store(h.now().UnixNano())
}

// Check returns an empty string if the heartbeat is subscribed and not older
// than maxAge, otherwise a description of the problem.
func (h *Heartbeat) Check() string {
	if !h.subscribed.Load() {
		return "heartbeat not subscribed"
	}
	age := h.now().Sub(time.Unix(0, h.last.Load()))
	if age > h.maxAge {
		return fmt.Sprintf("no heartbeat on %s for %s (max %s)", h.topic, age.Round(time.Second), h.maxAge)
	}
	return ""
}
