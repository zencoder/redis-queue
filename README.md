redis_queue
===========

A Go implementation of a FIFO queue backed by Redis


Installation
------------

Install redis_queue using the "go get" command:

    go get github.com/skidder/redis_queue/redis_queue

The Go distribution and [Redigo](https://github.com/garyburd/redigo) (a Go client for Redis) are the only dependencies.


Samples
-------

## Single-Queue Client

```go
package main

import "flag"
import "log"
import format "fmt"
import rq "github.com/skidder/redis_queue/redis_queue"

var (
	redisServer = flag.String("redisServer", ":6379", "Hostname and port for the Redis server")
	redisPassword = flag.String("redisPassword", "", "Password, optional")
)

func main() {
	flag.Parse();

	format.Printf("redisServer: %s\n", *redisServer)
	format.Printf("redisPassword: %s\n", *redisPassword)

	conn, err := rq.Connect(*redisServer)
	if err != nil {
		// handle it
	}

	queue_name := "example"
	queue := rq.CreateQueueConnection(conn, queue_name)

	// ensure the connection is closed on exit
	defer rq.Disconnect(conn)

	// push a simple value to the queue
	rq.Push(&queue, "bar")
	for {
		// print queue length
		length, err := rq.Length(&queue)
		format.Printf("%s: length: %d\n", queue_name, length)

		// remove an item from the queue
		rep, err := rq.Pop(&queue, 0)
		if err != nil {
			log.Println(err)
			log.Fatalf("Uh no!")
		}
		format.Printf("%s: message: %s\n", queue_name, rep)
	}
}
```

## Multi-Queue Client

```go
package main

import "fmt"
import rq "github.com/skidder/redis_queue/redis_queue"

func main() {
	addresses := make([]string, 2)
	addresses[0] = ":7777"
	addresses[1] = ":8777"

	queue_name := "example"
	queue:= rq.MultiQueueConnect(addresses, queue_name)

	// ensure the connection is closed on exit
	defer rq.MultiQueueDisconnect(&queue)

	// push a simple value to the queue
	rq.MultiPush(&queue, "bar")
	for {
		// print queue length
		length := rq.MultiLength(&queue)
		fmt.Printf("Queue length: %d\n", length)

		// remove an item from the queue
		rep, _ := rq.MultiPop(&queue, 1)
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

redis_queue is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
