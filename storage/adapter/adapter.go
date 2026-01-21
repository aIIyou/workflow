package adapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/aIIyou/workflow/event"
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
	CreateEvent(ctx context.Context, event *event.Event) error
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

// default ORM framework unified operation entrance
// the default orm framework is specified by the global variable framework
// user can use function SetFrameworkName to specify global default ORM framework
// note: global default ORM framework can only be specified once

func CreateEvent(ctx context.Context, event *event.Event) error {
	adapter, err := RetrieveAdapter(framework)
	if err != nil {
		return err
	}
	return adapter.CreateEvent(ctx, event)
}
