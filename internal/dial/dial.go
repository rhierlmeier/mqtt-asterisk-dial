package dial

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/ami"
	"mqtt-asterisk-dial/internal/config"
	"os"
	"sync/atomic"
	"text/template"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// originateWaitTimeout limits how long a triggered call waits for the AMI
// connection to come (back) up before it is given up.
var originateWaitTimeout = 2 * time.Minute

// originateRetryDelay is the pause between attempts while AMI is not connected.
var originateRetryDelay = 5 * time.Second

// Originator places calls via AMI. It is implemented by *ami.Client.
type Originator interface {
	Originate(ctx context.Context, req ami.OriginateRequest, onResult func(ami.OriginateResult)) error
}

type Dialer struct {
	mqttClient   mqtt.Client
	callFileDir  string
	callTemplate config.CallTemplate
	// originator is set in AMI mode; otherwise call files are written.
	originator Originator

	subscribeToken mqtt.Token
	subscribed     atomic.Bool

	variableValues map[string]interface{}
}

// NewDialer creates a dialer. If originator is nil, calls are placed by
// writing call files to callFileDir, otherwise via AMI Originate.
func NewDialer(mqttClient mqtt.Client, callFileDir string, callTemplate config.CallTemplate, originator Originator) (*Dialer, error) {

	if mqttClient == nil {
		return nil, fmt.Errorf("mqttClient cannot be nil")
	}
	if callTemplate.Topic == "" {
		return nil, fmt.Errorf("callTemplate.Topic cannot be empty")
	}

	return &Dialer{
		mqttClient:     mqttClient,
		callFileDir:    callFileDir,
		callTemplate:   callTemplate,
		originator:     originator,
		variableValues: make(map[string]interface{}),
	}, nil
}

// Subscribed reports whether all topics of the call are subscribed since the
// last (re)connect.
func (d *Dialer) Subscribed() bool {
	return d.subscribed.Load()
}

// MarkUnsubscribed must be called when the MQTT connection is lost.
func (d *Dialer) MarkUnsubscribed() {
	d.subscribed.Store(false)
}

// Start subscribes to the topics of the call. It must be called after every
// (re)connect, because the broker drops the subscriptions of a clean session.
func (d *Dialer) Start() error {
	d.subscribed.Store(false)

	for _, variable := range d.callTemplate.Variables {
		token := d.mqttClient.Subscribe(variable.Topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			d.onVariableChanged(variable.Name, string(msg.Payload()))
		})
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("call %s: could not subscribe to topic %s: %w", d.callTemplate.Name, variable.Topic, token.Error())
		}
		log.Printf("Call %s: Subscribed to topic %s for variable %s", d.callTemplate.Name, variable.Topic, variable.Name)
	}

	d.subscribeToken = d.mqttClient.Subscribe(d.callTemplate.Topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		d.onValueChanged(string(msg.Payload()))
	})
	if d.subscribeToken.Wait() && d.subscribeToken.Error() != nil {
		return fmt.Errorf("call %s: could not subscribe to topic %s: %w", d.callTemplate.Name, d.callTemplate.Topic, d.subscribeToken.Error())
	}
	log.Printf("Call %s: Subscribed to topic %s", d.callTemplate.Name, d.callTemplate.Topic)

	d.subscribed.Store(true)
	return nil
}

func (d *Dialer) onVariableChanged(name string, value string) {
	log.Printf("Call %s: Variable [%s] received: [%s]", d.callTemplate.Name, name, value)
	d.variableValues[name] = value
}

func (d *Dialer) onValueChanged(mqttValue string) {
	if d.callTemplate.Value != mqttValue {
		return
	}

	if d.originator != nil {
		log.Printf("Call %s: Value [%s] received, originating call via AMI", d.callTemplate.Name, mqttValue)
		// Copy the variables: originate runs concurrently to the MQTT handlers.
		vars := make(map[string]interface{}, len(d.variableValues))
		for name, value := range d.variableValues {
			vars[name] = value
		}
		// Do not block the MQTT handler while waiting for AMI.
		go d.originate(vars)
		return
	}

	log.Printf("Call %s: Value [%s] received, creating call file", d.callTemplate.Name, mqttValue)
	d.writeCallFile()
}

func (d *Dialer) writeCallFile() {
	callFileContent, err := render("callfile", d.callTemplate.CallFileTemplate, d.variableValues)
	if err != nil {
		log.Printf("Could not render call template: %v", err)
		return
	}

	tempFile, err := os.CreateTemp(d.callFileDir, "callfile-*.call")
	if err != nil {
		log.Printf("Could not create call file in %s: %v", d.callFileDir, err)
		return
	}
	defer tempFile.Close()

	os.Chmod(tempFile.Name(), 0644)

	_, err = tempFile.Write([]byte(callFileContent))
	if err != nil {
		log.Printf("Error writing call file %s: %v", tempFile.Name(), err)
		return
	}
	tempFile.Close()
}

// buildOriginateRequest renders the originate template with the MQTT variables.
func buildOriginateRequest(ot *config.OriginateTemplate, vars map[string]interface{}) (ami.OriginateRequest, error) {
	req := ami.OriginateRequest{
		Priority:  ot.Priority,
		Timeout:   time.Duration(ot.Timeout) * time.Second,
		CallerID:  ot.CallerID,
		Variables: make(map[string]string, len(ot.Setvar)),
	}
	var err error
	if req.Channel, err = render("channel", ot.Channel, vars); err != nil {
		return req, err
	}
	if req.Context, err = render("context", ot.Context, vars); err != nil {
		return req, err
	}
	if req.Exten, err = render("exten", ot.Exten, vars); err != nil {
		return req, err
	}
	for name, value := range ot.Setvar {
		if req.Variables[name], err = render("setvar "+name, value, vars); err != nil {
			return req, err
		}
	}
	return req, nil
}

func (d *Dialer) originate(vars map[string]interface{}) {
	name := d.callTemplate.Name

	req, err := buildOriginateRequest(d.callTemplate.Originate, vars)
	if err != nil {
		log.Printf("Call %s: Could not render originate template: %v", name, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), originateWaitTimeout)
	defer cancel()

	onResult := func(res ami.OriginateResult) {
		if res.Success {
			log.Printf("Call %s: OriginateResponse Success (reason %s, uniqueid %s)", name, res.Reason, res.Uniqueid)
		} else {
			log.Printf("Call %s: OriginateResponse Failure (reason %s)", name, res.Reason)
		}
	}

	for {
		err := d.originator.Originate(ctx, req, onResult)
		if err == nil {
			log.Printf("Call %s: Originate accepted (channel %s)", name, req.Channel)
			return
		}
		// Only retry if Asterisk cannot have seen the request; otherwise a
		// retry could place the call twice.
		if !errors.Is(err, ami.ErrNotConnected) {
			log.Printf("Call %s: Originate failed: %v", name, err)
			return
		}
		log.Printf("Call %s: Originate not sent (%v), retrying in %s", name, err, originateRetryDelay)
		select {
		case <-ctx.Done():
			log.Printf("Call %s: Giving up, AMI not available within %s", name, originateWaitTimeout)
			return
		case <-time.After(originateRetryDelay):
		}
	}
}

func render(name string, text string, vars map[string]interface{}) (string, error) {
	tmpl, err := template.New(name).Parse(text)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", name, err)
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, vars); err != nil {
		return "", fmt.Errorf("execute %s: %w", name, err)
	}
	return out.String(), nil
}
