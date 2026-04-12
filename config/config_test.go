package config

import "testing"

func Test_parseConfigFile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
	}{
		{
			name:     "test",
			fileName: "config.test.yaml",
		},
	}
	SetConfigPath("./config.test.yaml")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetConfigPath(tt.fileName)
			parseConfigFile()
			config := globalConfigure
			if config.MaxWorker != 20 {
				t.Errorf("MaxWorkers not match")
			}
			for _, testFlow := range config.Flow {
				if testFlow.FlowName != "test_flow" {
					t.Errorf("test_flow name not match")
				}
				if len(testFlow.Event) != 4 {
					t.Errorf("test_flow events not match")
				}
				if len(testFlow.Transitions) != 3 {
					t.Errorf("test_flow transitions not match")
				}
			}

		})
	}
}
