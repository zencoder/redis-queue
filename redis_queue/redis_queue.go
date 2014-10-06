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

import redis "github.com/garyburd/redigo/redis"

type Queue struct {
	conn redis.Conn
	key string
}

// Connect opens a connection to a Redis server and returns the connection.
// The connection should be closed by invoking Disconnect(conn),
// likely with defer.
func Connect(address string) (redis.Conn, error) {
	return redis.Dial("tcp", address)
}

// Close the Redis connection
func Disconnect(conn redis.Conn) {
	conn.Close()
}


func CreateQueueConnection(conn redis.Conn, key string) (Queue) {
	return Queue{conn:conn, key:key}
}

// Push will perform a right-push onto a Redis list/queue with the supplied 
// key and value.  An error will be returned if the operation failed.
func Push(queue Queue, value string) (error) {
	err := queue.conn.Send("RPUSH", queue.key, value)
	if err == nil {
		return queue.conn.Flush()
	} else {
		return err
	}
}

// Pop will perform a blocking left-pop from a Redis list/queue with the supplied 
// key.  An error will be returned if the operation failed.
func Pop(queue Queue, timeout int) (string, error) {
	rep, err := redis.Strings(queue.conn.Do("BLPOP", queue.key, timeout))
	if err == nil {
		return rep[1], nil
	} else {
		return "", err
	}
}

// Length will return the number of items in the specified list/queue
func Length(queue Queue) (int, error) {
	rep, err := redis.Int(queue.conn.Do("LLEN", queue.key))
	if err == nil {
		return rep, nil
	} else {
		return 0, err
	}
}
