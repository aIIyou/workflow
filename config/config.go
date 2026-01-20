package config

import (
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

const (
	configKey = "workflow"
)

var (
	globalPath           string //global configure file path
	globalConfigure      *Configuration
	globalConfigureMutex sync.RWMutex
)

type transition struct {
	FromEvent string `yaml:"from_event"`
	ToEvent   string `yaml:"to_event"`
	Expr      string `yaml:"expr"`
}

type Configuration struct {
	MaxWorkers int          `yaml:"max_workers"`
	Flows      []FlowConfig `yaml:"flows"`
}

type FlowConfig struct {
	FlowName    string       `yaml:"flow_name"`
	EventsName  []string     `yaml:"events_name"`
	Transitions []transition `yaml:"transitions"`
}

func SetConfigPath(path string) {
	globalPath = path
}

func GetConfigPath() string {
	return globalPath
}

func GetConfigure() *Configuration {
	globalConfigureMutex.Lock()
	defer globalConfigureMutex.Unlock()
	if globalConfigure == nil {
		parseConfigFile()
	}
	return globalConfigure
}

func parseConfigFile() {
	fileInfo, err := os.Stat(globalPath)
	if err != nil {
		panic(err)
	}
	if fileInfo.IsDir() {
		panic("xxx")
	}

	file, err := os.Open(globalPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var configData map[string]interface{}
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&configData); err != nil {
		panic(err)
	}
	if workflowConfig, exists := configData[configKey]; exists {
		workflowData, err := yaml.Marshal(workflowConfig)
		if err != nil {
			panic(err)
		}

		globalConfigure = new(Configuration)

		if err := yaml.Unmarshal(workflowData, globalConfigure); err != nil {
			panic(err)
		}
	}
}
