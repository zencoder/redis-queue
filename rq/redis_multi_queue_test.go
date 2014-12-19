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
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

func TestMultiQueueConnectOneHostSuccessful(t *testing.T) {
	pool := createPool()
	defer pool.Close()
	_, err := MultiQueueConnect([]*redis.Pool{pool}, "rq_test_queue")
	if err != nil {
		t.Error("Error while connecting to Redis", err)
	}
}

func TestMultiQueueConnectMultipleHostSuccessful(t *testing.T) {
	pool1 := createPool()
	defer pool1.Close()
	pool2 := createPool()
	defer pool2.Close()
	_, err := MultiQueueConnect([]*redis.Pool{pool1, pool2}, "rq_test_queue")
	if err != nil {
		t.Error("Error while connecting to Redis", err)
	}
}

func TestMultiQueueConnectFailure(t *testing.T) {
	pool := NewPool(":123", 1, 1, 240*time.Second)
	defer pool.Close()
	q, _ := MultiQueueConnect([]*redis.Pool{pool}, "rq_test_queue")
	_, err := q.Length()
	if err == nil {
		t.Error("Expected error connecting to Redis")
	}
}

func TestMultiQueuePushSuccessful(t *testing.T) {
	pool1 := createPool()
	defer pool1.Close()
	pool2 := createPool()
	defer pool2.Close()
	q, _ := MultiQueueConnect([]*redis.Pool{pool1, pool2}, "rq_test_pop_queue")

	err := q.Push("foo")
	if err != nil {
		t.Error("Error while pushing to Redis queue", err)
	}
}

func TestMultiQueuePopSuccessful(t *testing.T) {
	pool1 := createPool()
	defer pool1.Close()
	pool2 := createPool()
	defer pool2.Close()

	var e error
	if e = deleteKey(pool1, "rq_test_multi_pop_queue"); e != nil {
		t.Error("Unable to delete key in test setup")
	}

	q, err := MultiQueueConnect([]*redis.Pool{pool1, pool2}, "rq_test_multi_pop_queue")
	q.Push("foo")
	q.Push("bar")

	var value string
	value, err = q.Pop(1)
	if value != "foo" {
		t.Error("Expected foo but got: ", value)
	}
	if err != nil {
		t.Error("Unexpected error: ", err)
	}

	value, err = q.Pop(1)
	if value != "bar" {
		t.Error("Expected bar but got: ", value)
	}
	if err != nil {
		t.Error("Unexpected error: ", err)
	}

	if e = deleteKey(pool1, "rq_test_multi_pop_queue"); e != nil {
		t.Error("Unable to delete key in test cleanup")
	}
}

func TestMultiQueueLengthSuccessful(t *testing.T) {
	pool := createPool()
	defer pool.Close()

	var e error
	if e = deleteKey(pool, "rq_test_multiqueue_length"); e != nil {
		t.Error("Unable to delete key in test setup")
	}

	q, _ := MultiQueueConnect([]*redis.Pool{pool}, "rq_test_multiqueue_length")

	l, err := q.Length()
	if l != 0 {
		t.Error("Expect length to be 0, was: ", l)
	}
	if err != nil {
		t.Error("Error while getting length of Redis queue", err)
	}

	q.Push("foo")
	l, err = q.Length()

	if l != 1 {
		t.Error("Expect length to be 1, was: ", l)
	}
	if err != nil {
		t.Error("Error while getting length of Redis queue", err)
	}

	q.Pop(1)
	l, err = q.Length()
	if l != 0 {
		t.Error("Expect length to be 0, was: ", l)
	}
	if err != nil {
		t.Error("Error while getting length of Redis queue", err)
	}

	if e = deleteKey(pool, "rq_test_multiqueue_length"); e != nil {
		t.Error("Unable to delete key in test cleanup")
	}
}

func BenchmarkMultiQueuePushPop(b *testing.B) {
	pool1 := createPool()
	defer pool1.Close()
	pool2 := createPool()
	defer pool2.Close()

	var e error
	if e = deleteKey(pool1, "rq_test_multi_queue_pushpop_bench"); e != nil {
		b.Error("Unable to delete key in test setup")
	}

	q, _ := MultiQueueConnect([]*redis.Pool{pool1, pool2}, "rq_test_multi_queue_pushpop_bench")
	for i := 0; i < b.N; i++ {
		q.Push("foo")
		q.Pop(1)
	}

	if e = deleteKey(pool1, "rq_test_multi_queue_pushpop_bench"); e != nil {
		b.Error("Unable to delete key in test cleanup")
	}
}

func BenchmarkMultiQueueLength(b *testing.B) {
	pool1 := createPool()
	defer pool1.Close()
	pool2 := createPool()
	defer pool2.Close()

	var e error
	if e = deleteKey(pool1, "rq_test_queue_length_bench"); e != nil {
		b.Error("Unable to delete key in test setup")
	}

	q, _ := MultiQueueConnect([]*redis.Pool{pool1, pool2}, "rq_test_queue_length_bench")
	q.Push("foo")
	for i := 0; i < b.N; i++ {
		q.Length()
	}
	q.Pop(1)

	if e = deleteKey(pool1, "rq_test_queue_length_bench"); e != nil {
		b.Error("Unable to delete key in test cleanup")
	}
}

func deleteKey(pool *redis.Pool, key string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func createPoolWithConnectString(connectString string) *redis.Pool {
	return NewPool(connectString, 1, 1, 240*time.Second)
}

func createPool() *redis.Pool {
	return createPoolWithConnectString(":6379")
}
