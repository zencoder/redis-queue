redis-queue
===========

A Go implementation of a FIFO queue backed by Redis


Installation
------------

Install redis-queue using the "go get" command:

    go get github.com/skidder/redis-queue/rq

The Go distribution and [Redigo](https://github.com/garyburd/redigo) (a Go client for Redis) are the only dependencies.


Samples
-------

## Single-Queue Client

```go
package main

import "flag"
import "fmt"
import "log"
import rq "github.com/skidder/redis-queue/rq"

var (
	redisServer = flag.String("redisServer", ":6379", "Hostname and port for the Redis server")
	redisPassword = flag.String("redisPassword", "", "Password, optional")
)

func main() {
	flag.Parse();

	fmt.Printf("redisServer: %s\n", *redisServer)
	fmt.Printf("redisPassword: %s\n", *redisPassword)

	queue_name := "example"
	q, err := rq.Connect(*redisServer, queue_name)
	if err != nil {
		log.Fatal(err)
	}

	// ensure the connection is closed on exit
	defer q.Disconnect()

	// push a simple value to the queue
	q.Push("bar")
	for {
		// print queue length
		length, err := q.Length()
		fmt.Printf("%s: length: %d\n", queue_name, length)

		// remove an item from the queue
		rep, err := q.Pop(0)
		if err != nil {
			log.Println(err)
			log.Fatalf("Uh no!")
		}
		fmt.Printf("%s: message: %s\n", queue_name, rep)
	}
}
```

## Multi-Queue Client

```go
package main

import "fmt"
import "log"
import rq "github.com/skidder/redis-queue/rq"

func main() {
	addresses := make([]string, 2)
	addresses[0] = ":7777"
	addresses[1] = ":8777"

	queue_name := "example"
	queue, error := rq.MultiQueueConnect(addresses, queue_name)
	if error != nil {
		log.Fatal(error)
	}

	// ensure the connection is closed on exit
	defer queue.MultiQueueDisconnect()

	// push a simple value to the queue
	queue.MultiPush("bar")
	for {
		// print queue length
		length  := queue.MultiLength()
		fmt.Printf("%s: length: %d\n", queue_name, length)

		// remove an item from the queue
		rep, _ := queue.MultiPop(1)
		if rep != "" {
			fmt.Printf("Received message: %s\n", rep)
		} else {
			fmt.Printf("Queue is empty\n")
		}
	}
}
```


License
-------

redis-queue is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
