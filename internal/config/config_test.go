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
				Broker:      "broker",
				ClientId:    "client_id",
				Username:    "username",
				Password:    "password",
				CallFileDir: "./",
				Calls: []CallTemplate{
					{
						Name:             "call1",
						Topic:            "topic1",
						Value:            "value1",
						CallFileTemplate: "template1",
						Variables: []CallVariable{
							{Topic: "topic1", Name: "name1"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty broker",
			config: Config{
				ClientId:    "client_id",
				Username:    "username",
				Password:    "password",
				CallFileDir: "./",
				Calls: []CallTemplate{
					{
						Name:             "call1",
						Topic:            "topic1",
						Value:            "value1",
						CallFileTemplate: "template1",
						Variables: []CallVariable{
							{Topic: "topic1", Name: "name1"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty call_file_dir",
			config: Config{
				Broker:   "broker",
				ClientId: "client_id",
				Username: "username",
				Password: "password",
				Calls: []CallTemplate{
					{
						Name:             "call1",
						Topic:            "topic1",
						Value:            "value1",
						CallFileTemplate: "template1",
						Variables: []CallVariable{
							{Topic: "topic1", Name: "name1"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty calls",
			config: Config{
				Broker:      "broker",
				ClientId:    "client_id",
				Username:    "username",
				Password:    "password",
				CallFileDir: "./",
			},
			wantErr: true,
		},
		{
			name: "invalid call template",
			config: Config{
				Broker:      "broker",
				ClientId:    "client_id",
				Username:    "username",
				Password:    "password",
				CallFileDir: "./",
				Calls: []CallTemplate{
					{
						Name:             "",
						Topic:            "topic1",
						Value:            "value1",
						CallFileTemplate: "template1",
						Variables: []CallVariable{
							{Topic: "topic1", Name: "name1"},
						},
					},
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

func TestCallTemplateValidate(t *testing.T) {
	tests := []struct {
		name         string
		callTemplate CallTemplate
		wantErr      bool
	}{
		{
			name: "valid call template",
			callTemplate: CallTemplate{
				Name:             "call1",
				Topic:            "topic1",
				Value:            "value1",
				CallFileTemplate: "template1",
				Variables: []CallVariable{
					{Topic: "topic1", Name: "name1"},
				},
			},
			wantErr: false,
		},
		{
			name: "empty name",
			callTemplate: CallTemplate{
				Name:             "",
				Topic:            "topic1",
				Value:            "value1",
				CallFileTemplate: "template1",
				Variables: []CallVariable{
					{Topic: "topic1", Name: "name1"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty topic",
			callTemplate: CallTemplate{
				Name:             "call1",
				Topic:            "",
				Value:            "value1",
				CallFileTemplate: "template1",
				Variables: []CallVariable{
					{Topic: "topic1", Name: "name1"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty template",
			callTemplate: CallTemplate{
				Name:             "call1",
				Topic:            "topic1",
				Value:            "value1",
				CallFileTemplate: "",
				Variables: []CallVariable{
					{Topic: "topic1", Name: "name1"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty variable name",
			callTemplate: CallTemplate{
				Name:             "call1",
				Topic:            "topic1",
				Value:            "value1",
				CallFileTemplate: "template1",
				Variables: []CallVariable{
					{Topic: "topic1", Name: ""},
				},
			},
			wantErr: true,
		},
		{
			name: "empty variable topic",
			callTemplate: CallTemplate{
				Name:             "call1",
				Topic:            "topic1",
				Value:            "value1",
				CallFileTemplate: "template1",
				Variables: []CallVariable{
					{Topic: "", Name: "name1"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.callTemplate.Validate(false); (err != nil) != tt.wantErr {
				t.Errorf("CallTemplate.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
