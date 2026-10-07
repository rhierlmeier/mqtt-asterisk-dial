package config

import (
	"os"
	"path/filepath"
	"testing"
)

func validAmiConfig() Config {
	return Config{
		Broker: "broker",
		Ami:    &AmiConfig{Host: "asterisk", Username: "dialer", Secret: "secret"},
		Calls: []CallTemplate{
			{
				Name:  "call1",
				Topic: "topic1",
				Value: "true",
				Originate: &OriginateTemplate{
					Channel: "Local/start@chain",
					Context: "end",
					Exten:   "s",
				},
			},
		},
	}
}

func TestConfigValidateAmi(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(c *Config)
		wantErr bool
	}{
		{name: "valid, no call_file_dir needed", modify: func(c *Config) {}},
		{name: "empty host", modify: func(c *Config) { c.Ami.Host = "" }, wantErr: true},
		{name: "empty username", modify: func(c *Config) { c.Ami.Username = "" }, wantErr: true},
		{name: "empty secret", modify: func(c *Config) { c.Ami.Secret = "" }, wantErr: true},
		{name: "missing originate", modify: func(c *Config) { c.Calls[0].Originate = nil }, wantErr: true},
		{name: "empty channel", modify: func(c *Config) { c.Calls[0].Originate.Channel = "" }, wantErr: true},
		{name: "empty context", modify: func(c *Config) { c.Calls[0].Originate.Context = "" }, wantErr: true},
		{name: "empty exten", modify: func(c *Config) { c.Calls[0].Originate.Exten = "" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validAmiConfig()
			tt.modify(&c)
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigValidateAmiDefaults(t *testing.T) {
	c := validAmiConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Ami.Port != 5038 {
		t.Errorf("port = %d, want 5038", c.Ami.Port)
	}
	if c.Calls[0].Originate.Priority != 1 {
		t.Errorf("priority = %d, want 1", c.Calls[0].Originate.Priority)
	}
	if c.Calls[0].Originate.Timeout != 30 {
		t.Errorf("timeout = %d, want 30", c.Calls[0].Originate.Timeout)
	}
}

func TestLoadFromFileAmiSecretFromEnv(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(file, []byte(`
broker: tcp://localhost:1883
ami:
  host: asterisk
  username: dialer
calls:
  - name: call1
    topic: topic1
    value: "true"
    originate:
      channel: Local/start@chain
      context: end
      exten: s
      timeout: 200
      setvar:
        __stoerNr: "{{ .stoerNr }}"
    variables:
      - name: stoerNr
        topic: topic2
`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("AMI_SECRET", "")
	c := Config{}
	if err := c.LoadFromFile(file); err == nil {
		t.Errorf("expected error without secret")
	}

	t.Setenv("AMI_SECRET", "from-env")
	c = Config{}
	if err := c.LoadFromFile(file); err != nil {
		t.Fatal(err)
	}
	if c.Ami.Secret != "from-env" {
		t.Errorf("secret = %q, want from-env", c.Ami.Secret)
	}
	if got := c.Calls[0].Originate.Setvar["__stoerNr"]; got != "{{ .stoerNr }}" {
		t.Errorf("setvar = %q", got)
	}
	if c.Calls[0].Originate.Timeout != 200 {
		t.Errorf("timeout = %d, want 200", c.Calls[0].Originate.Timeout)
	}
}
