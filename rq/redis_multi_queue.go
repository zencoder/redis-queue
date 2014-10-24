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

import "errors"
import "log"
import "strings"
import "time"
import redis "github.com/garyburd/redigo/redis"

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
		conn, e := redis.Dial("tcp", urlParts[0])
		if e == nil {
			queues = append(queues, &ErrorDecayQueue{conn: conn, address: address, errorRatingTime: time.Now().Unix(), errorRating: 0.0})
		} else {
			log.Fatal(e)
		}

		if len(urlParts) == 2 {
			log.Println("Selecting database: ", urlParts[1])
			selectError := conn.Send("SELECT", urlParts[1])
			if selectError != nil {
				log.Fatal(selectError)
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
func (queue *MultiQueue) MultiQueueDisconnect() {
	for _, queue := range queue.queues {
		queue.conn.Close()
	}
}

// Push will perform a right-push onto a Redis list/queue with the supplied
// key and value.  An error will be returned if the operation failed.
func (multi_queue *MultiQueue) MultiPush(value string) error {
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
func (multi_queue *MultiQueue) MultiPop(timeout int) (string, error) {
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
func (multi_queue *MultiQueue) MultiLength() int {
	count := 0
	for _, queue := range multi_queue.HealthyQueues() {
		rep, err := redis.Int(queue.conn.Do("LLEN", multi_queue.key))
		if err == nil {
			count = count + rep
		}
	}
	return count
}
