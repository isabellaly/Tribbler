/*
	Author:
	Yi Li
	Fusheng Yuan
*/
package libstore

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type stringarr []string

func (a stringarr) Len() int {
	return len(a)
}
func (a stringarr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
	return
}
func (a stringarr) Less(i, j int) bool {
	icolon := strings.Index(a[i], ":")
	jcolon := strings.Index(a[j], ":")
	iTimeStamp, _ := strconv.ParseInt(a[i][icolon+1:], 10, 64)
	jTimeStamp, _ := strconv.ParseInt(a[j][jcolon+1:], 10, 64)
	return iTimeStamp > jTimeStamp
}

type nodearr []storagerpc.Node

func (a nodearr) Len() int {
	return len(a)
}
func (a nodearr) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
	return
}
func (a nodearr) Less(i, j int) bool {
	return a[i].NodeID < a[j].NodeID
}

type sCacheElem struct {
	validTill int64
	str       string
}

type lCacheElem struct {
	validTill int64
	list      []string
}

type libstore struct {
	masterServerHostPort string
	myHostPort           string
	leaseMode            LeaseMode
	sCache               map[string]*sCacheElem
	lCache               map[string]*lCacheElem
	queries              map[string]*list.List
	servers              []storagerpc.Node
	rpcClients           []*rpc.Client
	sCacheLock           *sync.Mutex
	lCacheLock           *sync.Mutex
	queriesLock          *sync.Mutex
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	var args storagerpc.GetServersArgs
	var reply storagerpc.RegisterReply
	masterServer, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, errors.New("Could not connect to master server")
	}
	masterServer.Call("StorageServer.GetServers", &args, &reply)
	if reply.Status == storagerpc.NotReady {
		for i := 0; i < 5; i++ {
			time.Sleep(1 * time.Second)
			masterServer.Call("StorageServer.GetServers", &args, &reply)
			if reply.Status == storagerpc.OK {
				break
			}
		}
	}
	masterServer.Close()
	if reply.Status == storagerpc.NotReady {
		return nil, nil
	}
	ls := &libstore{}
	ls.masterServerHostPort = masterServerHostPort
	ls.myHostPort = myHostPort
	ls.leaseMode = mode
	if myHostPort == "" {
		ls.leaseMode = Never
	}
	ls.sCache = make(map[string]*sCacheElem)
	ls.lCache = make(map[string]*lCacheElem)
	ls.queries = make(map[string]*list.List)
	ls.servers = reply.Servers
	sort.Sort(nodearr(ls.servers))
	ls.rpcClients = make([]*rpc.Client, len(ls.servers))
	for i := 0; i < len(ls.servers); i++ {
		ls.rpcClients[i], err = rpc.DialHTTP("tcp", ls.servers[i].HostPort)
	}
	ls.sCacheLock = new(sync.Mutex)
	ls.lCacheLock = new(sync.Mutex)
	ls.queriesLock = new(sync.Mutex)
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, errors.New("Could not register Libstore")
	}
	go ls.ClearCache()
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	currTime := time.Now().Unix()
	ls.sCacheLock.Lock()
	if sCacheElem, ok := ls.sCache[key]; ok {
		if sCacheElem.validTill >= currTime {
			ls.sCacheLock.Unlock()
			return sCacheElem.str, nil
		} else {
			delete(ls.sCache, key)
		}
	}
	ls.sCacheLock.Unlock()
	getArgs := &storagerpc.GetArgs{}
	getArgs.Key = key
	getArgs.WantLease = false
	getArgs.HostPort = ls.myHostPort
	if ls.leaseMode == Normal {
		ls.queriesLock.Lock()
		if pastQueries, ok := ls.queries[key]; ok {
			pastQueries.PushBack(currTime)
		} else {
			ls.queries[key] = list.New()
			ls.queries[key].PushBack(currTime)
		}
		if queryList, ok := ls.queries[key]; ok {
			for q := queryList.Front(); q != nil; q = q.Next() {
				if q.Value.(int64)+storagerpc.QueryCacheSeconds < currTime {
					queryList.Remove(q)
				} else {
					break
				}
			}
			if queryList.Len() >= storagerpc.QueryCacheThresh {
				getArgs.WantLease = true
			}
		}
		ls.queriesLock.Unlock()
	} else if ls.leaseMode == Always {
		getArgs.WantLease = true
	}

	hash := ls.GetHash(key)
	serverID := ls.LocateServer(hash)
	client := ls.rpcClients[serverID]
	var getReply storagerpc.GetReply
	client.Call("StorageServer.Get", getArgs, &getReply)
	if getReply.Status != storagerpc.OK {
		return "", errors.New(strconv.Itoa(int(getReply.Status)))
	}
	if getReply.Lease.Granted {
		s := &sCacheElem{}
		s.validTill = int64(getReply.Lease.ValidSeconds) + time.Now().Unix()
		s.str = getReply.Value
		ls.sCacheLock.Lock()
		ls.sCache[key] = s
		ls.sCacheLock.Unlock()
	}
	return getReply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	putArgs := &storagerpc.PutArgs{}
	putArgs.Key = key
	putArgs.Value = value
	hash := ls.GetHash(key)
	serverID := ls.LocateServer(hash)
	client := ls.rpcClients[serverID]
	var putReply storagerpc.PutReply
	client.Call("StorageServer.Put", putArgs, &putReply)
	if putReply.Status != storagerpc.OK {
		return errors.New(strconv.Itoa(int(putReply.Status)))
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	currTime := time.Now().Unix()
	ls.lCacheLock.Lock()
	if lCacheElem, ok := ls.lCache[key]; ok {
		if lCacheElem.validTill >= currTime {
			ls.lCacheLock.Unlock()
			return lCacheElem.list, nil
		} else {
			delete(ls.lCache, key)
		}
	}
	ls.lCacheLock.Unlock()
	getArgs := &storagerpc.GetArgs{}
	getArgs.Key = key
	getArgs.WantLease = false
	getArgs.HostPort = ls.myHostPort
	if ls.leaseMode == Normal {
		ls.queriesLock.Lock()
		if pastQueries, ok := ls.queries[key]; ok {
			pastQueries.PushBack(currTime)
		} else {
			ls.queries[key] = list.New()
			ls.queries[key].PushBack(currTime)
		}
		if queryList, ok := ls.queries[key]; ok {
			for q := queryList.Front(); q != nil; q = q.Next() {
				if q.Value.(int64)+storagerpc.QueryCacheSeconds < currTime {
					queryList.Remove(q)
				} else {
					break
				}
			}
			if queryList.Len() >= storagerpc.QueryCacheThresh {
				getArgs.WantLease = true
			}
		}
		ls.queriesLock.Unlock()
	} else if ls.leaseMode == Always {
		getArgs.WantLease = true
	}

	hash := ls.GetHash(key)
	serverID := ls.LocateServer(hash)
	client := ls.rpcClients[serverID]
	var getListReply storagerpc.GetListReply
	client.Call("StorageServer.GetList", getArgs, &getListReply)
	if getListReply.Status != storagerpc.OK {
		return nil, errors.New(strconv.Itoa(int(getListReply.Status)))
	}
	if getListReply.Lease.Granted {
		l := &lCacheElem{}
		l.validTill = int64(getListReply.Lease.ValidSeconds) + time.Now().Unix()
		l.list = getListReply.Value
		ls.lCacheLock.Lock()
		ls.lCache[key] = l
		ls.lCacheLock.Unlock()
	}
	return getListReply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	putArgs := &storagerpc.PutArgs{}
	putArgs.Key = key
	putArgs.Value = removeItem
	hash := ls.GetHash(key)
	serverID := ls.LocateServer(hash)
	client := ls.rpcClients[serverID]
	var putReply storagerpc.PutReply
	client.Call("StorageServer.RemoveFromList", putArgs, &putReply)
	if putReply.Status != storagerpc.OK {
		return errors.New(strconv.Itoa(int(putReply.Status)))
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	putArgs := &storagerpc.PutArgs{}
	putArgs.Key = key
	putArgs.Value = newItem
	hash := ls.GetHash(key)
	serverID := ls.LocateServer(hash)
	client := ls.rpcClients[serverID]
	var putReply storagerpc.PutReply
	client.Call("StorageServer.AppendToList", putArgs, &putReply)
	if putReply.Status != storagerpc.OK {
		return errors.New(strconv.Itoa(int(putReply.Status)))
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.sCacheLock.Lock()
	if _, ok := ls.sCache[args.Key]; ok {
		delete(ls.sCache, args.Key)
		reply.Status = storagerpc.OK
		ls.sCacheLock.Unlock()
		return nil
	}
	ls.sCacheLock.Unlock()
	ls.lCacheLock.Lock()
	if _, ok := ls.lCache[args.Key]; ok {
		delete(ls.lCache, args.Key)
		reply.Status = storagerpc.OK
		ls.lCacheLock.Unlock()
		return nil
	}
	ls.lCacheLock.Unlock()
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func (ls *libstore) GetHash(key string) uint32 {
	hash := StoreHash(key)
	index := strings.Index(key, ":")
	if index != -1 {
		hash = StoreHash(key[:index])
	}
	return hash
}

func (ls *libstore) LocateServer(hash uint32) int {
	serverID := 0
	for i := 0; i < len(ls.servers); i++ {
		if i == len(ls.servers)-1 {
			return 0
		}
		if hash > ls.servers[i].NodeID && hash <= ls.servers[i+1].NodeID {
			serverID = i + 1
			break
		}
	}
	return serverID
}

func (ls *libstore) ClearCache() {
	for {
		currTime := time.Now().Unix()
		ls.sCacheLock.Lock()
		for k, v := range ls.sCache {
			if v.validTill < currTime {
				delete(ls.sCache, k)
			}
		}
		ls.sCacheLock.Unlock()
		ls.lCacheLock.Lock()
		for k, v := range ls.lCache {
			if v.validTill < currTime {
				delete(ls.lCache, k)
			}
		}
		ls.lCacheLock.Unlock()
		time.Sleep(1 * time.Second)
	}
}
