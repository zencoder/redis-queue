redis-queue [![Circle CI](https://circleci.com/gh/skidder/redis-queue.png?style=badge)](https://circleci.com/gh/skidder/redis-queue)
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
import "github.com/skidder/redis-queue/rq"

var (
	redisServer = flag.String("redisServer", ":6379", "Hostname and port for the Redis server")
	redisPassword = flag.String("redisPassword", "", "Password, optional")
)

func main() {
	flag.Parse();

	fmt.Printf("redisServer: %s\n", *redisServer)
	fmt.Printf("redisPassword: %s\n", *redisPassword)

	pool := rq.newPool(":6379")
	defer pool.Close()

	queue_name := "example"
	q, err := rq.QueueConnect(pool, queue_name)
	if err != nil {
		log.Fatal(err)
	}

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
import "github.com/skidder/redis-queue/rq"

func main() {
	pool1 := rq.newPool(":7777")
	defer pool1.Close()

	pool2 := rq.newPool(":8777")
	defer pool2.Close()

	queue_name := "example"
	q, err := rq.MultiQueueConnect([]redis.Pool{*pool1, *pool2}, queue_name)
	if error != nil {
		log.Fatal(error)
	}

	// push a simple value to the queue
	q.Push("bar")
	for {
		// print queue length
		length  := q.Length()
		fmt.Printf("%s: length: %d\n", queue_name, length)

		// remove an item from the queue
		rep, _ := q.Pop(1)
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
