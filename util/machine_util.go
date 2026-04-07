package util

type MachineUtil interface {
	LocalIP() string
}

type BoundMachineUtil struct {
}

func (bmu BoundMachineUtil) LocalIP() string {
	return ""
}
