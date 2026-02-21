package config

import (
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Broker:   "tcp://localhost:1883",
				ClientId: "client_id",
				SIP: SIPConfig{
					Host: "1.2.3.4",
				},
				Numbers: []string{"123"},
				Messages: []Message{
					{
						MqttTopic: "topic",
						MqttValues: []MqttValue{
							{Value: "true", AudioFile: "file.wav"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty broker",
			config: Config{
				SIP: SIPConfig{
					Host: "1.2.3.4",
				},
				Messages: []Message{
					{MqttTopic: "topic"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty sip host",
			config: Config{
				Broker: "tcp://localhost:1883",
				Messages: []Message{
					{MqttTopic: "topic"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty messages",
			config: Config{
				Broker: "tcp://localhost:1883",
				SIP: SIPConfig{
					Host: "1.2.3.4",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
