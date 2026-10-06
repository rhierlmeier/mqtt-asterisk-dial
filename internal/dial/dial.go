package dial

import (
	"bytes"
	"fmt"
	"log"
	"mqtt-asterisk-dial/internal/config"
	"os"
	"text/template"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Dialer struct {
	mqttClient   mqtt.Client
	callFileDir  string
	callTemplate config.CallTemplate

	subscribeToken mqtt.Token

	variableValues map[string]interface{}
}

func NewDialer(mqttClient mqtt.Client, callFileDir string, callTemplate config.CallTemplate) (*Dialer, error) {

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
		variableValues: make(map[string]interface{}),
	}, nil
}

// Start subscribes to the topics of the call. It must be called after every
// (re)connect, because the broker drops the subscriptions of a clean session.
func (d *Dialer) Start() error {

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

	return nil
}

func (d *Dialer) onVariableChanged(name string, value string) {
	log.Printf("Call %s: Variable [%s] received: [%s]", d.callTemplate.Name, name, value)
	d.variableValues[name] = value
}

func (d *Dialer) onValueChanged(mqttValue string) {

	if d.callTemplate.Value == mqttValue {

		log.Printf("Call %s: Value [%s] received, creating call file", d.callTemplate.Name, mqttValue)

		tmpl, err := template.New("callfile").Parse(d.callTemplate.CallFileTemplate)
		if err != nil {
			log.Printf("Could not parse call template: %v", err)
			return
		}

		var callFileContent bytes.Buffer
		err = tmpl.Execute(&callFileContent, d.variableValues)
		if err != nil {
			log.Printf("Could not execute call template: %v", err)
			return
		}
		tempFile, err := os.CreateTemp(d.callFileDir, "callfile-*.call")
		if err != nil {
			log.Printf("Could not create temp file %s: %v", tempFile.Name(), err)
			return
		}
		defer tempFile.Close()

		os.Chmod(tempFile.Name(), 0644)

		_, err = tempFile.Write(callFileContent.Bytes())
		if err != nil {
			log.Printf("Error writing call file %s: %v", tempFile.Name(), err)
			return
		}
		tempFile.Close()
	}

}
