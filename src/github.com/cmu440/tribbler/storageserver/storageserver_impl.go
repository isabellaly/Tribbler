/*
	Author:
	Fusheng Yuan
	Yi Li
*/
package storageserver

import (
	"container/list"
	"encoding/json"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type storageServer struct {
	// TODO: implement this!
	tribbleHash          map[string][]byte
	listHash             map[string]*list.List
	portmun              int
	nodeID               uint32
	isMaster             bool
	nodes                map[storagerpc.Node]bool
	callbackConnections  map[string]*rpc.Client
	modification         map[string]bool
	modificationList     map[string]bool
	leases               map[string]*list.List
	leasesList           map[string]*list.List
	connections          map[string]*rpc.Client
	masterServerHostPort string
	numNodes             int
	connectionLock       *sync.Mutex
	leaseLocker          *sync.Mutex
	leaseListLocker      *sync.Mutex
	modifyingLock        *sync.Mutex
	modifyingListLock    *sync.Mutex
	registeredLocker     *sync.Mutex
	storageLocker        *sync.Mutex
	listLocker           *sync.Mutex
	callbackLocker       *sync.Mutex
}

type leaseInfo struct {
	address    string
	expiryTime time.Time
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	serverNode := &storageServer{
		tribbleHash:          make(map[string][]byte),
		listHash:             make(map[string]*list.List),
		modification:         make(map[string]bool),
		modificationList:     make(map[string]bool),
		leases:               make(map[string]*list.List),
		leasesList:           make(map[string]*list.List),
		connections:          make(map[string]*rpc.Client),
		nodes:                make(map[storagerpc.Node]bool),
		portmun:              port,
		nodeID:               nodeID,
		numNodes:             numNodes,
		masterServerHostPort: masterServerHostPort,
		connectionLock:       new(sync.Mutex),
		leaseLocker:          new(sync.Mutex),
		leaseListLocker:      new(sync.Mutex),
		modifyingLock:        new(sync.Mutex),
		modifyingListLock:    new(sync.Mutex),
		registeredLocker:     new(sync.Mutex),
		storageLocker:        new(sync.Mutex),
		listLocker:           new(sync.Mutex),
		callbackLocker:       new(sync.Mutex),
		isMaster:             false,
		callbackConnections:  make(map[string]*rpc.Client),
	}

	if serverNode.masterServerHostPort == "" {
		serverNode.isMaster = true
		serverNode.masterServerHostPort = "localhost:" + strconv.Itoa(port)
		rpc.RegisterName("StorageServer", serverNode)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		go http.Serve(l, nil)
	}
	for {
		client, err := rpc.DialHTTP("tcp", serverNode.masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{*&storagerpc.Node{"localhost:" + strconv.Itoa(port), nodeID}}
		var reply *storagerpc.RegisterReply
		err = client.Call("StorageServer.RegisterServer", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			if !serverNode.isMaster {
				for _, node := range reply.Servers {
					serverNode.nodes[node] = true
				}
			}
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	if !serverNode.isMaster {
		serverNode.masterServerHostPort = "localhost:" + strconv.Itoa(port)
		rpc.RegisterName("StorageServer", serverNode)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		go http.Serve(l, nil)
	}
	return serverNode, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.registeredLocker.Lock()
	defer ss.registeredLocker.Unlock()
	ss.nodes[args.ServerInfo] = true
	if ss.numNodes == len(ss.nodes) {
		reply.Servers = make([]storagerpc.Node, ss.numNodes)
		reply.Status = storagerpc.OK
		i := 0
		for node, _ := range ss.nodes {
			reply.Servers[i] = node
			i++
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.registeredLocker.Lock()
	defer ss.registeredLocker.Unlock()
	if ss.numNodes == len(ss.nodes) {
		reply.Servers = make([]storagerpc.Node, ss.numNodes)
		reply.Status = storagerpc.OK
		i := 0
		for node, _ := range ss.nodes {
			reply.Servers[i] = node
			i++
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil

}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.checkValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if args.WantLease {
		ss.modifyingLock.Lock()
		if ss.modification[args.Key] == true {
			reply.Lease.Granted = false
		} else {
			reply.Lease.Granted = true
			ss.leaseLocker.Lock()
			lst := ss.leases[args.Key]
			if lst == nil {
				lst = list.New()
			}
			lst.PushBack(&leaseInfo{args.HostPort, time.Now().Add((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)})
			ss.leases[args.Key] = lst
			ss.leaseLocker.Unlock()
		}
		ss.modifyingLock.Unlock()
	}
	ss.storageLocker.Lock()
	defer ss.storageLocker.Unlock()
	if ss.tribbleHash[args.Key] == nil {
		reply.Status = storagerpc.KeyNotFound
	} else {
		var value string
		json.Unmarshal(ss.tribbleHash[args.Key], &value)
		reply.Value = value
		reply.Status = storagerpc.OK
	}
	return nil
}
func (ss *storageServer) checkValid(Key string) bool {
	hostName := strings.Split(Key, ":")
	hash := StoreHash(hostName[0])
	return ss.validServer(hash)
}

func (ss *storageServer) validServer(keyHash uint32) bool {
	var serverID, minID uint32
	serverID = 1<<32 - 1
	minID = 1<<32 - 1
	for node, _ := range ss.nodes {
		if node.NodeID >= keyHash && node.NodeID < serverID {
			serverID = node.NodeID
		}
		if minID > node.NodeID {
			minID = node.NodeID
		}
	}
	if serverID == 1<<32-1 {
		serverID = minID
	}
	if serverID == ss.nodeID {
		return true
	} else {
		return false
	}
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.checkValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if args.WantLease {
		ss.modifyingListLock.Lock()
		if ss.modificationList[args.Key] == true {
			reply.Lease.Granted = false
		} else {
			reply.Lease.Granted = true
			ss.leaseListLocker.Lock()
			lst := ss.leasesList[args.Key]
			if lst == nil {
				lst = list.New()
			}

			lst.PushBack(&leaseInfo{args.HostPort, time.Now().Add((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second)})
			ss.leasesList[args.Key] = lst
			ss.leaseListLocker.Unlock()
		}
		ss.modifyingListLock.Unlock()
	}

	ss.listLocker.Lock()
	defer ss.listLocker.Unlock()
	if ss.listHash[args.Key] == nil {
		reply.Status = storagerpc.KeyNotFound
	} else {
		valueList := ss.listHash[args.Key]
		reply.Value = make([]string, valueList.Len())
		i := 0
		for e := valueList.Front(); e != nil; e = e.Next() {
			reply.Value[i] = e.Value.(string)
			i++
		}
		reply.Status = storagerpc.OK
	}

	return nil

}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("start put")
	if !ss.checkValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.modifyingLock.Lock()
	ss.modification[args.Key] = true
	ss.modifyingLock.Unlock()

	ss.revokeLease(args.Key, false)
	ss.storageLocker.Lock()
	ss.tribbleHash[args.Key], _ = json.Marshal(args.Value)
	ss.storageLocker.Unlock()

	ss.modifyingLock.Lock()
	delete(ss.modification, args.Key)
	ss.modifyingLock.Unlock()
	reply.Status = storagerpc.OK
	//fmt.Println.Println("finish put")
	return nil
}

func (ss *storageServer) revokeLease(key string, isList bool) {
	var list *list.List
	if isList {
		ss.leaseListLocker.Lock()
		list = ss.leasesList[key]
		ss.leaseListLocker.Unlock()
	} else {
		ss.leaseLocker.Lock()
		list = ss.leases[key]
		ss.leaseLocker.Unlock()
	}
	if list == nil {
		return
	}
	for e := list.Front(); e != nil; e = e.Next() {
		info := e.Value.(*leaseInfo)
		if !time.Now().After(info.expiryTime) {
			ss.connectLibStore(key, info)
		}
	}
	if isList {
		ss.leaseListLocker.Lock()
		delete(ss.leasesList, key)
		ss.leaseListLocker.Unlock()
	} else {
		ss.leaseLocker.Lock()
		delete(ss.leases, key)
		ss.leaseLocker.Unlock()
	}
	//fmt.Println.Println("finish revoke")
}

func (ss *storageServer) connectLibStore(key string, info *leaseInfo) {
	doneCh := make(chan bool)
	go func() {
		ss.callbackLocker.Lock()
		cli := ss.callbackConnections[info.address]
		if cli == nil {
			cli, _ = rpc.DialHTTP("tcp", info.address)
			ss.callbackConnections[info.address] = cli
		}
		ss.callbackLocker.Unlock()
		var args storagerpc.RevokeLeaseArgs
		var reply storagerpc.RevokeLeaseReply
		args.Key = key
		cli.Call("LeaseCallbacks.RevokeLease", &args, &reply)
		doneCh <- true
	}()
	select {
	case <-doneCh:
		break
	case <-time.After((storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second):
		break
	}
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	//fmt.Println.Println("start AppendToList")

	ss.modifyingListLock.Lock()
	ss.modificationList[args.Key] = true
	ss.modifyingListLock.Unlock()
	ss.revokeLease(args.Key, true)

	ss.storageLocker.Lock()
	curList := ss.listHash[args.Key]
	if curList == nil {
		ss.listHash[args.Key] = list.New()
		curList = ss.listHash[args.Key]
	} else {
		for e := curList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Value {
				reply.Status = storagerpc.ItemExists
				ss.storageLocker.Unlock()
				return nil
			}
		}
	}
	curList.PushBack(args.Value)
	ss.storageLocker.Unlock()
	reply.Status = storagerpc.OK

	ss.modifyingListLock.Lock()
	delete(ss.modificationList, args.Key)
	ss.modifyingListLock.Unlock()

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.checkValid(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.modifyingListLock.Lock()
	ss.modificationList[args.Key] = true
	ss.modifyingListLock.Unlock()
	ss.revokeLease(args.Key, true)

	ss.storageLocker.Lock()
	curList := ss.listHash[args.Key]
	if curList != nil {
		for e := curList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Value {
				curList.Remove(e)
				ss.storageLocker.Unlock()
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	ss.storageLocker.Unlock()
	reply.Status = storagerpc.ItemNotFound

	ss.modifyingListLock.Lock()
	delete(ss.modificationList, args.Key)
	ss.modifyingListLock.Unlock()

	return nil
}

func StoreHash(key string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}
