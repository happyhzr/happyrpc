package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/happyhzr/happyrpc"
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
	client, _ := happyrpc.Dial("tcp", <-addr)
	defer func() {
		client.Close()
	}()
	time.Sleep(time.Second)
	wg := &sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := fmt.Sprintf("happyrpc req %d", i)
			reply := ""
			err := client.Call("Foo.Sum", args, &reply)
			if err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Println("reply:", reply)
		}(i)
	}
	wg.Wait()
}
