package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Broker   string `yaml:"broker"`
	ClientId string `yaml:"client_id"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	// CallFileDir is the directory for Asterisk call files (call file mode).
	CallFileDir string `yaml:"call_file_dir"`

	// Ami switches to AMI mode: calls are placed via the Originate action
	// instead of call files.
	Ami *AmiConfig `yaml:"ami"`

	// HealthListen is the listen address of the health endpoints
	// (e.g. "127.0.0.1:8080"). Empty disables the endpoints.
	HealthListen string `yaml:"health_listen"`

	// Heartbeat makes /readyz fail when no message arrives on a topic that
	// is published to periodically.
	Heartbeat *HeartbeatConfig `yaml:"heartbeat"`

	Calls []CallTemplate `yaml:"calls"`
}

// HeartbeatConfig configures the heartbeat check.
type HeartbeatConfig struct {
	Topic string `yaml:"topic"`
	// MaxAge in seconds after the last heartbeat (default 900).
	MaxAge int `yaml:"max_age"`
}

func (h *HeartbeatConfig) Validate() error {
	if h.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if h.MaxAge == 0 {
		h.MaxAge = 900
	}
	return nil
}

// AmiConfig configures the connection to the Asterisk Manager Interface.
type AmiConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	// Secret of the manager user. The environment variable AMI_SECRET
	// overrides it.
	Secret string `yaml:"secret"`
}

type CallTemplate struct {
	Name  string `yaml:"name"`
	Topic string `yaml:"topic"`
	Value string `yaml:"value"`
	// CallFileTemplate is the Go template of the call file (call file mode).
	CallFileTemplate string `yaml:"template"`
	// Originate describes the Originate action (AMI mode).
	Originate *OriginateTemplate `yaml:"originate"`
	Variables []CallVariable     `yaml:"variables"`
}

// OriginateTemplate describes an AMI Originate action. Channel, Context, Exten
// and the values of Setvar are Go templates with access to the MQTT variables.
type OriginateTemplate struct {
	Channel  string `yaml:"channel"`
	Context  string `yaml:"context"`
	Exten    string `yaml:"exten"`
	Priority int    `yaml:"priority"`
	// Timeout in seconds Asterisk waits for the channel to be answered
	// (default 30).
	Timeout  int    `yaml:"timeout"`
	CallerID string `yaml:"caller_id"`
	// Setvar sets channel variables; prefix the name with "__" to make the
	// variable inheritable, e.g. "__stoerNr".
	Setvar map[string]string `yaml:"setvar"`
}

type CallVariable struct {
	Topic string `yaml:"topic"`
	Name  string `yaml:"name"`
}

func (c *Config) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	ret := decoder.Decode(c)

	if secret := os.Getenv("AMI_SECRET"); secret != "" && c.Ami != nil {
		c.Ami.Secret = secret
	}

	if err := c.Validate(); err != nil {
		return err
	}

	return ret
}

func checkDirExists(dir string) error {
	fileInfo, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", dir)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("path %s is not a directory", dir)
	}
	return nil
}

func (ct *CallTemplate) Validate(amiMode bool) error {
	if ct.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}

	if ct.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	if amiMode {
		if ct.Originate == nil {
			return fmt.Errorf("originate cannot be empty when ami is configured")
		}
		if err := ct.Originate.Validate(); err != nil {
			return fmt.Errorf("originate: %v", err)
		}
	} else if ct.CallFileTemplate == "" {
		return fmt.Errorf("template cannot be empty")
	}

	for varIndex, variable := range ct.Variables {
		if variable.Name == "" {
			return fmt.Errorf("variables[%d].name cannot be empty", varIndex)
		}
		if variable.Topic == "" {
			return fmt.Errorf("variables[%d].topic cannot be empty", varIndex)
		}
	}
	return nil
}

func (ot *OriginateTemplate) Validate() error {
	if ot.Channel == "" {
		return fmt.Errorf("channel cannot be empty")
	}
	if ot.Context == "" {
		return fmt.Errorf("context cannot be empty")
	}
	if ot.Exten == "" {
		return fmt.Errorf("exten cannot be empty")
	}
	if ot.Priority == 0 {
		ot.Priority = 1
	}
	if ot.Timeout == 0 {
		ot.Timeout = 30
	}
	return nil
}

func (a *AmiConfig) Validate() error {
	if a.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}
	if a.Port == 0 {
		a.Port = 5038
	}
	if a.Username == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if a.Secret == "" {
		return fmt.Errorf("secret cannot be empty (set it in the config or via AMI_SECRET)")
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

	if c.Ami != nil {
		if err := c.Ami.Validate(); err != nil {
			return fmt.Errorf("invalid ami: %v", err)
		}
	} else {
		if c.CallFileDir == "" {
			return fmt.Errorf("call_file_dir cannot be empty")
		}

		if err := checkDirExists(c.CallFileDir); err != nil {
			return fmt.Errorf("invalid call_file_dir: %v", err)
		}
	}

	if c.Heartbeat != nil {
		if err := c.Heartbeat.Validate(); err != nil {
			return fmt.Errorf("invalid heartbeat: %v", err)
		}
	}

	if len(c.Calls) == 0 {
		return fmt.Errorf("calls cannot be empty")
	}

	for callIndex := range c.Calls {
		if err := c.Calls[callIndex].Validate(c.Ami != nil); err != nil {
			return fmt.Errorf("calls[%d]: %v", callIndex, err)
		}
	}

	return nil
}
