// Copyright 2014 Brighcove Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package rq provides a simple queue abstraction that is backed by Redis.
package rq

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"
)

type MultiQueue struct {
	key    string
	queues []*ErrorDecayQueue
}

var noQueuesAvailableError = errors.New("No queues available")

// Connect opens a connection to a Redis server and returns the connection.
// The connection should be closed by invoking Disconnect(conn),
// likely with defer.
func MultiQueueConnect(addresses []string, key string) (MultiQueue, error) {
	if len(addresses) == 0 {
		return MultiQueue{}, errors.New("No connection addresses provided, aborting")
	}

	queues := []*ErrorDecayQueue{}
	for _, address := range addresses {
		urlParts := strings.Split(address, "/")
		log.Println("Connecting to Redis server: ", urlParts[0])
		conn, connectErr := redis.Dial("tcp", urlParts[0])
		if connectErr == nil {
			queues = append(queues, &ErrorDecayQueue{conn: conn, address: address, errorRatingTime: time.Now().Unix(), errorRating: 0.0})
		} else {
			return MultiQueue{}, connectErr
		}

		if len(urlParts) == 2 {
			log.Println("Selecting database: ", urlParts[1])
			selectErr := conn.Send("SELECT", urlParts[1])
			if selectErr != nil {
				return MultiQueue{}, selectErr
			}
		}
	}

	if len(queues) == 0 {
		return MultiQueue{}, errors.New("No queue connections could be made")
	} else {
		return MultiQueue{key: key, queues: queues}, nil
	}
}

// Close the Redis connection
func (queue *MultiQueue) Disconnect() error {
	log.Println("Disconnecting from Redis servers")
	var disconnectErr error
	for _, queue := range queue.queues {
		err := queue.conn.Close()
		if err != nil {
			disconnectErr = err
		}
	}
	return disconnectErr
}

// Push will perform a right-push onto a Redis list/queue with the supplied
// key and value.  An error will be returned if the operation failed.
func (multi_queue *MultiQueue) Push(value string) error {
	selected_queue, err := multi_queue.SelectHealthyQueue()
	if err != nil {
		return err
	}

	push_error := selected_queue.conn.Send("RPUSH", multi_queue.key, value)
	if push_error == nil {
		return selected_queue.conn.Flush()
	} else {
		selected_queue.QueueError()
		return push_error
	}
}

// Pop will perform a blocking left-pop from a Redis list/queue with the supplied
// key.  An error will be returned if the operation failed.
func (multi_queue *MultiQueue) Pop(timeout int) (string, error) {
	selected_queue, err := multi_queue.SelectHealthyQueue()
	if err != nil {
		return "", err
	}

	rep, err := selected_queue.conn.Do("BLPOP", multi_queue.key, timeout)
	if err == nil {
		r, err := redis.Strings(rep, err)
		if err == nil {
			return r[1], nil
		}
		return "", err
	} else {
		selected_queue.QueueError()
		return "", err
	}
}

// Length will return the number of items in the specified list/queue
func (multi_queue *MultiQueue) Length() (int, error) {
	count := 0
	for _, queue := range multi_queue.HealthyQueues() {
		rep, err := redis.Int(queue.conn.Do("LLEN", multi_queue.key))
		if err == nil {
			count = count + rep
		} else {
			return count, err
		}
	}
	return count, nil
}

func (mq *MultiQueue) HealthyQueues() []*ErrorDecayQueue {
	now := time.Now().Unix()
	healthyQueues := []*ErrorDecayQueue{}
	for _, queue := range mq.queues {
		timeDelta := now - queue.errorRatingTime
		updatedErrorRating := queue.errorRating * math.Exp((math.Log(0.5)/10)*float64(timeDelta))

		if updatedErrorRating < 0.1 {
			if queue.errorRating >= 0.1 {
				// transitioning the queue out of an unhealthy state, try reconnecting
				queue.conn.Close()
				conn, error := redis.Dial("tcp", queue.address)
				if error == nil {
					queue.conn = conn
					healthyQueues = append(healthyQueues, queue)
				}
			} else {
				healthyQueues = append(healthyQueues, queue)
			}
		}
		queue.errorRatingTime = time.Now().Unix()
		queue.errorRating = updatedErrorRating
	}
	return healthyQueues
}

func (mq *MultiQueue) SelectHealthyQueue() (*ErrorDecayQueue, error) {
	healthyQueues := mq.HealthyQueues()
	numberOfHealthyQueues := len(healthyQueues)

	index := 0
	if numberOfHealthyQueues == 0 {
		numberOfQueues := len(mq.queues)
		if numberOfQueues == 0 {
			return nil, noQueuesAvailableError
		}
		index = rand.Intn(numberOfQueues)
		return mq.queues[index], nil
	} else if numberOfHealthyQueues > 1 {
		index = rand.Intn(numberOfHealthyQueues)
	}
	return healthyQueues[index], nil
}
