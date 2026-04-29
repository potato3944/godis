package server

import (
	"log"
	"net"
	"net/rpc"
	"testing"

	"godis/api"
	"godis/store"
)

func startBenchmarkServer() (string, func()) {
	s := store.NewConcurrentMap(0, store.PolicyNone)
	kvServer := NewKVServer(s, nil, nil)
	
	server := rpc.NewServer()
	err := server.RegisterName("KVServer", kvServer)
	if err != nil {
		log.Fatalf("Register error: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	return l.Addr().String(), func() {
		l.Close()
	}
}

func BenchmarkKVServer_Set(b *testing.B) {
	addr, cleanup := startBenchmarkServer()
	defer cleanup()

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("Dial error: %v", err)
	}
	defer client.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := &api.SetArgs{Key: "key", Value: "value"}
		var reply api.SetReply
		err := client.Call("KVServer.Set", args, &reply)
		if err != nil {
			b.Fatalf("Call error: %v", err)
		}
	}
}

func BenchmarkKVServer_Get(b *testing.B) {
	addr, cleanup := startBenchmarkServer()
	defer cleanup()

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("Dial error: %v", err)
	}
	defer client.Close()

	// Pre-set a value
	argsSet := &api.SetArgs{Key: "key", Value: "value"}
	var replySet api.SetReply
	client.Call("KVServer.Set", argsSet, &replySet)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := &api.GetArgs{Key: "key"}
		var reply api.GetReply
		err := client.Call("KVServer.Get", args, &reply)
		if err != nil {
			b.Fatalf("Call error: %v", err)
		}
	}
}

func BenchmarkKVServer_Set_Parallel(b *testing.B) {
	addr, cleanup := startBenchmarkServer()
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("Dial error: %v", err)
		}
		defer client.Close()
		
		for pb.Next() {
			args := &api.SetArgs{Key: "key", Value: "value"}
			var reply api.SetReply
			err := client.Call("KVServer.Set", args, &reply)
			if err != nil {
				b.Fatalf("Call error: %v", err)
			}
		}
	})
}

func BenchmarkKVServer_Get_Parallel(b *testing.B) {
	addr, cleanup := startBenchmarkServer()
	defer cleanup()

	// Pre-set a value
	clientInit, err := rpc.Dial("tcp", addr)
	if err == nil {
		argsSet := &api.SetArgs{Key: "key", Value: "value"}
		var replySet api.SetReply
		clientInit.Call("KVServer.Set", argsSet, &replySet)
		clientInit.Close()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("Dial error: %v", err)
		}
		defer client.Close()

		for pb.Next() {
			args := &api.GetArgs{Key: "key"}
			var reply api.GetReply
			err := client.Call("KVServer.Get", args, &reply)
			if err != nil {
				b.Fatalf("Call error: %v", err)
			}
		}
	})
}
