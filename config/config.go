package config

import (
	"fmt"
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

type Transition struct {
	FromEvent string `yaml:"from_event"`
	ToEvent   string `yaml:"to_event"`
	Expr      string `yaml:"expr"`
}

type Configuration struct {
	MaxWorker int          `yaml:"max_worker"`
	Flow      []FlowConfig `yaml:"flow"`
}

type EventConfig struct {
	Name  string `yaml:"name"`
	Async bool   `yaml:"async"`
}

type FlowConfig struct {
	FlowName    string        `yaml:"flow_name"`
	Event       []EventConfig `yaml:"event"`
	StartEvent  string        `yaml:"start_event"`
	Transitions []Transition  `yaml:"transition"`
}

func SetConfigPath(path string) {
	globalPath = path
}

func GetConfigPath() string {
	return globalPath
}

func GetConfigure() *Configuration {
	globalConfigureMutex.RLock()
	if globalConfigure == nil {
		globalConfigureMutex.RUnlock()
		parseConfigFile()
	} else {
		globalConfigureMutex.RUnlock()
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
		globalConfigureMutex.Lock()
		defer globalConfigureMutex.Unlock()
		globalConfigure = new(Configuration)

		if err := yaml.Unmarshal(workflowData, globalConfigure); err != nil {
			panic(err)
		}
	} else {
		panic(fmt.Sprintf(`key "%s" not exists`, configKey))
	}
}
