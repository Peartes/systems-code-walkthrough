package rpc

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	gob "github.com/peartes/raft/labgob"
)

type reqMsg struct {
	data      []byte
	replyChan chan replyMsg
	endpoint  string
	service   string
	reqType   reflect.Type
}

type replyMsg struct {
	ok   bool
	data []byte
}

// ClientEnd represents a client connection to a server
type ClientEnd struct {
	name      string
	broadcast chan<- reqMsg   // channel to broadcast request to the network so it's a copy of the channel in the Network struct
	done      <-chan struct{} // channel to signal that the entire network has been destroyed
}

func (e *ClientEnd) Call(serviceMethod string, args any, reply any) bool {
	req := reqMsg{
		data:      []byte{},
		replyChan: make(chan replyMsg, 1),
		service:   serviceMethod,
		endpoint:  e.name,
		reqType:   reflect.TypeOf(args),
	}
	// encode the request
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(args)
	if err != nil {
		log.Printf("labrpc.ClientEnd.Call: failed to encode args for serviceMethod %v: %v\n", serviceMethod, err)
		return false
	}
	req.data = buf.Bytes()
	// broadcast the request to the server on the network and await for the response
	select {
	case e.broadcast <- req:
		// the request has been sent.
	case <-e.done:
		// entire Network has been destroyed.
		return false
	}
	var rep replyMsg
	select {
	case rep = <-req.replyChan:
		if !rep.ok {
			log.Printf("labrpc.ClientEnd.Call: received error reply for serviceMethod %v\n", serviceMethod)
			return false
		}
	case <-e.done:
		return false
	}
	// decode the response into reply
	buf = bytes.NewBuffer(rep.data)
	decoder := gob.NewDecoder(buf)
	err = decoder.Decode(reply)
	if err != nil {
		log.Printf("labrpc.ClientEnd.Call: failed to decode reply for serviceMethod %v: %v\n", serviceMethod, err)
		return false
	}
	return true
}

type Network struct {
	servers   map[string]*Server
	ends      map[string]*ClientEnd
	endServer map[string]string // maps endpoint name → destination server name
	endOwner  map[string]string // maps endpoint name → owner server name (who holds this ClientEnd)
	connected map[string]bool   // maps endpoint name → whether it can currently deliver messages
	broadcast chan reqMsg
	done      chan struct{} // cleanup when the entire network is destroyed
	mu        sync.Mutex
	reliable  bool // if false, requests are randomly dropped/delayed
}

func NewNetwork() *Network {
	network := &Network{
		servers:   make(map[string]*Server),
		ends:      make(map[string]*ClientEnd),
		endServer: make(map[string]string),
		endOwner:  make(map[string]string),
		connected: make(map[string]bool),
		broadcast: make(chan reqMsg),
		done:      make(chan struct{}),
	}
	go func() {
		for {
			select {
			case <-network.done:
				return
			case req, ok := <-network.broadcast:
				if !ok {
					return
				}
				// process the request and send the response back to the client
				network.processReq(req)
			}
		}
	}()
	return network
}

func (n *Network) processReq(req reqMsg) {
	if req.replyChan == nil {
		return
	}

	// Check connectivity before anything else.
	// A disconnected or crashed endpoint fails immediately with no delay —
	// simulating a severed connection rather than a timeout.
	n.mu.Lock()
	isConnected := n.connected[req.endpoint]
	n.mu.Unlock()
	if !isConnected {
		req.replyChan <- replyMsg{false, nil}
		return
	}

	if !n.reliable {
		// short delay
		ms := (rand.Int() % 27)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	if !n.reliable && (rand.Int()%1000) < 100 {
		// drop the request, return as if timeout
		req.replyChan <- replyMsg{false, nil}
		return
	}
	select {
	case <-n.done:
		req.replyChan <- replyMsg{ok: false, data: nil}
		close(req.replyChan)
		return
	default:
	}
	n.mu.Lock()
	srvName := n.endServer[req.endpoint]
	server := n.servers[srvName]
	n.mu.Unlock()
	if server == nil {
		req.replyChan <- replyMsg{ok: false, data: nil}
		close(req.replyChan)
		return
	}
	if req.reqType == nil {
		req.replyChan <- replyMsg{ok: false, data: nil}
		close(req.replyChan)
		return
	}
	// decode the req.data into an object
	argType := req.reqType
	if argType.Kind() == reflect.Ptr {
		argType = argType.Elem()
	}
	arg := reflect.New(argType)
	buf := bytes.NewReader(req.data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(arg.Interface()); err != nil {
		req.replyChan <- replyMsg{ok: false, data: nil}
		close(req.replyChan)
		return
	}
	// dispatch the request to the server
	go func() {
		rep := server.dispatch(req.service, arg.Elem().Interface())
		select {
		case req.replyChan <- rep:
		default:
		}
		close(req.replyChan)
	}()
}

func (n *Network) Destroy() {
	close(n.done)
}

func (n *Network) Reliable(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = yes
}

func (n *Network) AddServer(name string, server *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[name] = server
}

func (n *Network) DeleteServer(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, name)
}

func (n *Network) MakeEnd(endName string) *ClientEnd {
	end := &ClientEnd{name: endName, broadcast: n.broadcast, done: n.done}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ends[endName] = end
	n.connected[endName] = true // endpoints are connected by default
	return end
}

// SetEndOwner records which server owns (holds) this ClientEnd.
// This is the server that will be SENDING through this endpoint.
// Required for DisableServer/EnableServer to cut outbound traffic correctly.
func (n *Network) SetEndOwner(endName, serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.endOwner[endName] = serverName
}

// DisableServer cuts all connections to and from serverName.
// Affects: all ClientEnds owned by serverName (outbound) and
//          all ClientEnds whose destination is serverName (inbound).
func (n *Network) DisableServer(serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for endName := range n.ends {
		if n.endOwner[endName] == serverName || n.endServer[endName] == serverName {
			n.connected[endName] = false
		}
	}
}

// EnableServer restores all connections to and from serverName.
func (n *Network) EnableServer(serverName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for endName := range n.ends {
		if n.endOwner[endName] == serverName || n.endServer[endName] == serverName {
			n.connected[endName] = true
		}
	}
}

// Connect connects the client end with the server. The server must already be added to the network using AddServer before calling Connect.
func (n *Network) Connect(endName string, serverName string) {
	if _, ok := n.servers[serverName]; !ok {
		log.Fatalf("labrpc.Network.Connect: server %v does not exist in the network\n", serverName)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.endServer[endName] = serverName
}

type Server struct {
	mu       sync.Mutex
	name     string
	services map[string]*Service // service name to services
}

func NewServer(name string) *Server {
	return &Server{name: name, services: make(map[string]*Service)}
}

// handle dispatching the request to the appropriate service
// the req is to be the typed data the service method is expecting
func (s *Server) dispatch(service string, req any) replyMsg {
	s.mu.Lock()
	// split the service and method
	dot := strings.LastIndex(service, ".")
	serviceName := service[:dot]
	methodName := service[dot+1:]

	s.mu.Unlock()

	if srv, ok := s.services[serviceName]; ok {
		return srv.dispatch(methodName, reflect.ValueOf(req))
	} else {
		choices := []string{}
		for k, _ := range s.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func (s *Server) AddService(service *Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[service.name] = service
}

type Service struct {
	name    string
	recvr   reflect.Value // the object the method is applied on a.k.a self
	methods map[string]reflect.Method
}

func NewService(service any) *Service {
	// get the methods of the service
	srvc := &Service{}
	srvcType := reflect.TypeOf(service)
	fmt.Printf("labrpc.NewService: creating service for type %v\n", srvcType)
	srvc.recvr = reflect.ValueOf(service)
	srvc.methods = make(map[string]reflect.Method)
	srvc.name = reflect.Indirect(reflect.ValueOf(service)).Type().Name()
	for i := 0; i < srvcType.NumMethod(); i++ {
		method := srvcType.Method(i)
		if method.Type.NumIn() != 3 || method.Type.NumOut() != 0 {
			log.Printf("labrpc.NewService: method %v has wrong number of ins and outs\n skipping method", method.Name)
			continue
		}
		if method.Type.In(1).Kind() != reflect.Struct {
			log.Printf("labrpc.NewService: method %v argument 1 is not a struct\n skipping method", method.Name)
			continue
		} else if method.Type.In(2).Kind() != reflect.Ptr {
			log.Printf("labrpc.NewService: method %v argument 2 is not a pointer to a struct\n skipping method", method.Name)
			continue
		}
		srvc.methods[method.Name] = method
	}
	return srvc
}

func (service *Service) dispatch(mthdName string, request reflect.Value) replyMsg {
	// make sure the method exists in the service
	if method, ok := service.methods[mthdName]; ok {
		// to prevent the service from modifying the request, we create a copy of the request
		requestCopy := reflect.New(request.Type())
		requestCopy.Elem().Set(request)
		// create an object for the response
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)
		// call the method with the arg
		method.Func.Call([]reflect.Value{service.recvr, requestCopy.Elem(), replyv})
		// encode the response
		res := new(bytes.Buffer)
		encoder := gob.NewEncoder(res)
		encoder.Encode(replyv.Interface())
		return replyMsg{true, res.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range service.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			mthdName, service.methods, choices)
		return replyMsg{false, nil}
	}
}
