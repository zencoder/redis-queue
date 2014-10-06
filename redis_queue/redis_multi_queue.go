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

// Package redis_queue provides a simple queue abstraction that is backed by Redis.
package redis_queue

import "time"
import redis "github.com/garyburd/redigo/redis"

// Connect opens a connection to a Redis server and returns the connection.
// The connection should be closed by invoking Disconnect(conn),
// likely with defer.
func MultiConnect(addresses []string) ([]redis.Conn, error) {
	connections := make([]redis.Conn, len(addresses))
	for index, address := range addresses {
		conn, error := redis.Dial("tcp", address)
		if error != nil {
			return nil, error
		}
		connections[index] = conn
	}
	return connections, nil
}

// Close the Redis connection
func MultiDisconnect(connections []redis.Conn) {
	for _, conn := range connections {
		conn.Close()
	}
}

// Build a MultiQueueConnection structure for a specific queue
func CreateMultiQueueConnection(connections []redis.Conn, key string) (MultiQueue) {	
	queues := make([]ErrorDecayQueue, len(connections))
	for index, conn := range connections {
		queues[index] = ErrorDecayQueue{conn:conn, error_rating:0.0, error_rating_time:time.Now().Unix()}
	}

	healthy_queues := make([]ErrorDecayQueue, len(connections))
	copy(queues, healthy_queues)
	
	return MultiQueue{key:key, queues:queues}
}

// Push will perform a right-push onto a Redis list/queue with the supplied 
// key and value.  An error will be returned if the operation failed.
func MultiPush(multi_queue MultiQueue, value string) (error) {
	selected_queue, err := SelectHealthyQueue(multi_queue)
	if err != nil {
		return err
	}

	push_error := selected_queue.conn.Send("RPUSH", multi_queue.key, value)
	if push_error == nil {
		return selected_queue.conn.Flush()
	} else {
		QueueError(selected_queue)
		return push_error
	}
}

// Pop will perform a blocking left-pop from a Redis list/queue with the supplied 
// key.  An error will be returned if the operation failed.
func MultiPop(multi_queue MultiQueue, timeout int) (string, error) {
	selected_queue, err := SelectHealthyQueue(multi_queue)
	if err != nil {
		return "", err
	}

	rep, err := redis.Strings(selected_queue.conn.Do("BLPOP", multi_queue.key, timeout))
	if err == nil {
		return rep[1], nil
	} else {
		QueueError(selected_queue)
		return "", err
	}
}

// Length will return the number of items in the specified list/queue
func MultiLength(multi_queue MultiQueue) (int, error) {
	count := 0
	for _, queue := range multi_queue.queues {
		rep, err := redis.Int(queue.conn.Do("LLEN", multi_queue.key))
		if err == nil {
			count = count + rep
		} else {
			return count, err
		}
	}
	return count, nil
}
