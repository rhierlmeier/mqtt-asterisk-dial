package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker   string `yaml:"broker"`
	ClientId string `yaml:"client_id"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	SIP SIPConfig `yaml:"sip"`

	Numbers  []string  `yaml:"numbers"`
	Messages []Message `yaml:"messages"`
}

type SIPConfig struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type Message struct {
	MqttTopic string     `yaml:"mqtt_topic"`
	Variables []Variable `yaml:"variables"`
	AudioFile string     `yaml:"audio_file"`
}

type Variable struct {
	Name      string `yaml:"name"`
	MqttTopic string `yaml:"mqtt_topic"`
}

func (c *Config) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		return err
	}

	if err := c.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *Config) Validate() error {
	if c.Broker == "" {
		return fmt.Errorf("broker cannot be empty")
	}

	if c.ClientId == "" {
		c.ClientId = "mqtt-asterisk-dial"
	}

	if c.SIP.Host == "" {
		return fmt.Errorf("sip.host cannot be empty")
	}

	if len(c.Messages) == 0 {
		return fmt.Errorf("messages cannot be empty")
	}

	return nil
}
