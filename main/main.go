package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/happyhzr/happyrpc"
	"github.com/happyhzr/happyrpc/codec"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rcp server on", l.Addr())
	addr <- l.Addr().String()
	happyrpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)
	conn, _ := net.Dial("tcp", <-addr)
	defer conn.Close()
	time.Sleep(time.Second)
	json.NewEncoder(conn).Encode(happyrpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		cc.Write(h, fmt.Sprintf("happyrpc req %d", h.Seq))
		cc.ReadHeader(h)
		reply := ""
		cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
