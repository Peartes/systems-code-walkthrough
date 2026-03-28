package rpc

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gob "github.com/peartes/raft/labgob"
	"github.com/stretchr/testify/require"
)

type test struct{}
type rqArg struct{}
type replyArg struct {
	Data []byte
}

type otherService struct{}

func (s *otherService) Method1(req rqArg, reply *replyArg) {
	*reply = replyArg{Data: []byte("other response")}
}

type counterService struct {
	count int32
}

func (s *counterService) Method1(req rqArg, reply *replyArg) {
	atomic.AddInt32(&s.count, 1)
	*reply = replyArg{Data: []byte("ok")}
}

type blockingService struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingService) Method1(req rqArg, reply *replyArg) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	*reply = replyArg{Data: []byte("blocked")}
}

func (t *test) Method1(req rqArg, reply *replyArg) {
	*reply = replyArg{Data: []byte("Method1 response")}
}
func (t *test) Method2(req rqArg, reply *replyArg) {
	*reply = replyArg{Data: []byte("Method2 response")}
}

type noMethod struct{}

type wrongMethod struct{}

func (t *wrongMethod) Method1(req rqArg) {}

type wrongMethod2 struct{}

func (t *wrongMethod2) Method1(req string, reply string) {}

type wrongMethod3 struct{}

func (t *wrongMethod3) method1(req rqArg, reply *replyArg) {}

func (t *wrongMethod3) Method2(req rqArg, reply *replyArg) {}

func (t *wrongMethod3) Method3(req rqArg, reply *replyArg) {}

type wrongMethod4 struct{}

func (t *wrongMethod4) Method1(req rqArg, reply *replyArg) string {
	return "wrongMethod4 response"
}

func TestService(t *testing.T) {
	t.Run("create service passes on struct with no methods", func(*testing.T) {
		service := NewService(noMethod{})
		require.NotNil(t, service)
		require.Equal(t, service.name, "noMethod")
		require.Equal(t, service.recvr, reflect.ValueOf(noMethod{}))
		require.Empty(t, service.methods)
	})
	t.Run("create service with struct with methods", func(*testing.T) {
		service := NewService(&test{})
		require.NotNil(t, service)
		require.Equal(t, service.name, "test")
		require.Equal(t, service.recvr, reflect.ValueOf(&test{}))
		require.Len(t, service.methods, 2)
	})

	t.Run("service does not add methods without signature (*receiver) (arg1 any, arg2 any)", func(*testing.T) {
		service := NewService(&wrongMethod{})
		require.NotNil(t, service)
		require.Equal(t, service.name, "wrongMethod")
		require.Equal(t, service.recvr, reflect.ValueOf(&wrongMethod{}))
		require.Empty(t, service.methods)
	})

	t.Run("service does not add non exported methods", func(t *testing.T) {
		service := NewService(&wrongMethod3{})
		require.NotNil(t, service)
		require.Equal(t, service.name, "wrongMethod3")
		require.Equal(t, service.recvr, reflect.ValueOf(&wrongMethod3{}))
		require.Len(t, service.methods, 2)
	})

	t.Run("service does not add methods with a return type", func(t *testing.T) {
		service := NewService(&wrongMethod4{})
		require.NotNil(t, service)
		require.Equal(t, "wrongMethod4", service.name)
		require.Equal(t, service.recvr, reflect.ValueOf(&wrongMethod4{}))
		require.Empty(t, service.methods)
	})

	t.Run("service rejects methods with non-pointer arguments", func(*testing.T) {
		service := NewService(&wrongMethod2{})
		require.NotNil(t, service)
		require.Equal(t, "wrongMethod2", service.name)
		require.Equal(t, service.recvr, reflect.ValueOf(&wrongMethod2{}))
		require.Empty(t, service.methods)
	})

	t.Run("dispatch calls the correct method", func(*testing.T) {
		service := NewService(&test{})
		require.NotNil(t, service)
		reply := replyArg{}
		request := rqArg{}
		replyMsg := service.dispatch("Method1", reflect.ValueOf(request))
		require.True(t, replyMsg.ok)
		// decode the reply data
		err := gob.NewDecoder(bytes.NewReader(replyMsg.data)).Decode(&reply)
		require.NoError(t, err)
		require.Equal(t, replyArg{Data: []byte("Method1 response")}, reply)
	})
}

func TestServer(t *testing.T) {
	t.Run("create server initializes fields", func(*testing.T) {
		server := NewServer("s1")
		require.NotNil(t, server)
		require.Equal(t, "s1", server.name)
		require.NotNil(t, server.services)
		require.Empty(t, server.services)
	})

	t.Run("dispatch calls service method", func(*testing.T) {
		server := NewServer("s1")
		service := NewService(&test{})
		server.AddService(service)

		reply := replyArg{}
		request := rqArg{}
		replyMsg := server.dispatch("test.Method2", request)
		require.True(t, replyMsg.ok)

		err := gob.NewDecoder(bytes.NewReader(replyMsg.data)).Decode(&reply)
		require.NoError(t, err)
		require.Equal(t, replyArg{Data: []byte("Method2 response")}, reply)
	})

	t.Run("dispatch resolves the correct service among multiple services", func(*testing.T) {
		server := NewServer("s1")
		server.AddService(NewService(&test{}))
		server.AddService(NewService(&otherService{}))

		reply1 := replyArg{}
		replyMsg1 := server.dispatch("test.Method1", rqArg{})
		require.True(t, replyMsg1.ok)
		err := gob.NewDecoder(bytes.NewReader(replyMsg1.data)).Decode(&reply1)
		require.NoError(t, err)
		require.Equal(t, replyArg{Data: []byte("Method1 response")}, reply1)

		reply2 := replyArg{}
		replyMsg2 := server.dispatch("otherService.Method1", rqArg{})
		require.True(t, replyMsg2.ok)
		err = gob.NewDecoder(bytes.NewReader(replyMsg2.data)).Decode(&reply2)
		require.NoError(t, err)
		require.Equal(t, replyArg{Data: []byte("other response")}, reply2)
	})
}

func TestServerDispatchUnknownService(t *testing.T) {
	if os.Getenv("SERVER_DISPATCH_UNKNOWN_SERVICE") == "1" {
		server := NewServer("s1")
		server.dispatch("unknown.Method1", rqArg{})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestServerDispatchUnknownService$")
	cmd.Env = append(os.Environ(), "SERVER_DISPATCH_UNKNOWN_SERVICE=1")
	err := cmd.Run()
	require.Error(t, err)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
}

func TestNetworkLifecycle(t *testing.T) {
	t.Run("create add delete endpoint and call", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(true)
		require.NotNil(t, network)

		server := NewServer("s1")
		server.AddService(NewService(&test{}))
		network.AddServer("s1", server)
		require.Contains(t, network.servers, "s1")

		end := network.MakeEnd("end1")
		require.NotNil(t, end)
		network.Connect("end1", "s1")

		reply := replyArg{}
		ok := end.Call("test.Method1", rqArg{}, &reply)
		require.True(t, ok)
		require.Equal(t, replyArg{Data: []byte("Method1 response")}, reply)

		network.DeleteServer("s1")
		reply = replyArg{}
		ok = end.Call("test.Method1", rqArg{}, &reply)
		require.False(t, ok)
		require.Equal(t, replyArg{}, reply)
	})

	t.Run("multiple endpoints call appropriate services", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(true)
		server1 := NewServer("s1")
		server1.AddService(NewService(&test{}))
		server2 := NewServer("s2")
		server2.AddService(NewService(&otherService{}))
		network.AddServer("s1", server1)
		network.AddServer("s2", server2)

		end1 := network.MakeEnd("end1")
		end2 := network.MakeEnd("end2")
		network.Connect("end1", "s1")
		network.Connect("end2", "s2")

		reply1 := replyArg{}
		ok := end1.Call("test.Method2", rqArg{}, &reply1)
		require.True(t, ok)
		require.Equal(t, replyArg{Data: []byte("Method2 response")}, reply1)

		reply2 := replyArg{}
		ok = end2.Call("otherService.Method1", rqArg{}, &reply2)
		require.True(t, ok)
		require.Equal(t, replyArg{Data: []byte("other response")}, reply2)
	})

	t.Run("concurrent calls on multiple endpoints do not deadlock", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(true)
		service := &counterService{}
		server := NewServer("s1")
		server.AddService(NewService(service))
		network.AddServer("s1", server)

		end1 := network.MakeEnd("end1")
		end2 := network.MakeEnd("end2")
		network.Connect("end1", "s1")
		network.Connect("end2", "s1")

		const totalCalls = 100
		errCh := make(chan error, totalCalls)
		var wg sync.WaitGroup
		for i := 0; i < totalCalls; i++ {
			end := end1
			if i%2 == 0 {
				end = end2
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				reply := replyArg{}
				if ok := end.Call("counterService.Method1", rqArg{}, &reply); !ok {
					errCh <- errors.New("call failed")
				}
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for concurrent calls")
		}
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}
		require.Equal(t, int32(totalCalls), atomic.LoadInt32(&service.count))
	})

	t.Run("destroy during in-flight call returns false and nil reply", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(true)
		service := &blockingService{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		server := NewServer("s1")
		server.AddService(NewService(service))
		network.AddServer("s1", server)
		end := network.MakeEnd("end1")
		network.Connect("end1", "s1")

		resultCh := make(chan bool, 1)
		replyCh := make(chan replyArg, 1)
		go func() {
			reply := replyArg{}
			ok := end.Call("blockingService.Method1", rqArg{}, &reply)
			resultCh <- ok
			replyCh <- reply
		}()

		<-service.started
		network.Destroy()

		select {
		case ok := <-resultCh:
			require.False(t, ok)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for call to return after destroy")
		}

		close(service.release)
		reply := <-replyCh
		require.Nil(t, reply.Data)
	})

	t.Run("reliable network delivers all requests", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(true)
		service := &counterService{}
		server := NewServer("s1")
		server.AddService(NewService(service))
		network.AddServer("s1", server)
		end := network.MakeEnd("end1")
		network.Connect("end1", "s1")

		const totalCalls = 50
		for i := 0; i < totalCalls; i++ {
			reply := replyArg{}
			ok := end.Call("counterService.Method1", rqArg{}, &reply)
			require.True(t, ok)
		}
		require.Equal(t, int32(totalCalls), atomic.LoadInt32(&service.count))
	})

	t.Run("unreliable network drops some requests", func(t *testing.T) {
		network := NewNetwork()
		network.Reliable(false)
		server := NewServer("s1")
		server.AddService(NewService(&test{}))
		network.AddServer("s1", server)
		end := network.MakeEnd("end1")
		network.Connect("end1", "s1")

		const totalCalls = 200
		var success int32
		var failed int32
		var wg sync.WaitGroup
		for i := 0; i < totalCalls; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				reply := replyArg{}
				if ok := end.Call("test.Method1", rqArg{}, &reply); ok {
					atomic.AddInt32(&success, 1)
				} else {
					atomic.AddInt32(&failed, 1)
				}
			}()
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for unreliable calls")
		}

		require.Greater(t, success, int32(0))
		require.Greater(t, failed, int32(0))
	})
}
