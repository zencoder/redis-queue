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
	"testing"
	"time"
)

func TestQueueConnectSuccessful(t *testing.T) {
	pool := CreatePool()
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue")

	_, err := q.Length()
	if err != nil {
		t.Error("Error while connecting to Redis", err)
	}
}

func TestQueueConnectFailure(t *testing.T) {
	pool := NewPool(":123", 1, 1, 240*time.Second)
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue")

	_, err := q.Length()
	if err == nil {
		t.Error("Error while connecting to Redis", err)
	}
}

func TestQueuePushSuccessful(t *testing.T) {
	pool := CreatePool()
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue")

	err := q.Push("foo")
	if err != nil {
		t.Error("Error while pushing to Redis queue", err)
	}
}

func TestQueuePopSuccessful(t *testing.T) {
	pool := CreatePool()
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue_pushpop")

	q.Push("foo")
	q.Push("bar")

	var value string
	var err error
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
}

func BenchmarkQueuePushPop(b *testing.B) {
	pool := CreatePool()
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue_pushpop_bench")
	for i := 0; i < b.N; i++ {
		q.Push("foo")
		q.Pop(1)
	}
}

func BenchmarkQueueLength(b *testing.B) {
	pool := CreatePool()
	defer pool.Close()
	q := QueueConnect(pool, "rq_test_queue_length_bench")
	q.Push("foo")
	for i := 0; i < b.N; i++ {
		q.Length()
	}
	q.Pop(1)
}
