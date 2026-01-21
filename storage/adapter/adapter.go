package adapter

import (
	"context"
	"fmt"
	"sync"

	flow "github.com/aIIyou/workflow/event_flow"
)

type FrameworkName string

const (
	GF FrameworkName = "gf"
	GO FrameworkName = "gorm"
)

var (
	framework          = GF
	frameworkOnceMutex sync.Once
)

func SetFrameworkName(name FrameworkName) {
	f := func() {
		framework = name
	}
	frameworkOnceMutex.Do(f)
}

var (
	adapters      map[FrameworkName]Adapter
	adaptersMutex sync.RWMutex
)

type Adapter interface {

	//CreateEvent insert event into table `event_queue`
	//Canonical adapter must use transaction to make insert and user logic atomic.
	CreateEvent(ctx context.Context, event *flow.Event) error
}

func RegisterAdapter(name FrameworkName, adapter Adapter) error {
	adaptersMutex.Lock()
	defer adaptersMutex.Unlock()
	if adapters == nil {
		adapters = make(map[FrameworkName]Adapter)
	}
	if _, existed := adapters[name]; existed {
		panic(fmt.Sprintf(`%s adapter has already registered`, name))
	}
	adapters[name] = adapter
	return nil
}

func RetrieveAdapter(name FrameworkName) (Adapter, error) {
	adaptersMutex.RLock()
	defer adaptersMutex.RUnlock()
	if adapter, existed := adapters[name]; !existed {
		return nil, fmt.Errorf(`%s adapter has not been registered`, name)
	} else {
		return adapter, nil
	}
}
