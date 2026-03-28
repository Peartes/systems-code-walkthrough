package main

type service interface {
	Call()
}

type raftService struct{}

func (r *raftService) Call() {
	// Implementation of the Call method for raftService
}

func main() {
	var s service = &raftService{}
	s.Call()
}
