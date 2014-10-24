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
)

func TestQueueConnectSuccessful(t *testing.T) {
	_, err := QueueConnect(":6379", "rq_test_queue")
	if err != nil {
		t.Error("Error while connecting to Redis", err)
	}
}

func TestQueueConnectFailure(t *testing.T) {
	_, err := QueueConnect(":123", "rq_test_queue")
	if err == nil {
		t.Error("Expected error connecting to Redis")
	}
}

func TestQueueDisconnectSuccessful(t *testing.T) {
	q, err := QueueConnect(":6379", "rq_test_queue")
	q.Disconnect()
	if err != nil {
		t.Error("Error while disconnecting from Redis", err)
	}
}

func TestQueuePushSuccessful(t *testing.T) {
	q, _ := QueueConnect(":6379", "rq_test_queue")
	err := q.Push("foo")
	if err != nil {
		t.Error("Error while pushing to Redis queue", err)
	}
	q.Disconnect()
}

func TestQueuePopSuccessful(t *testing.T) {
	q, _ := QueueConnect(":6379", "rq_test_pop_queue")
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
	q.Disconnect()
}

func TestQueueLengthSuccessful(t *testing.T) {
	q, _ := QueueConnect(":6379", "rq_test_queue_length")

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

	q.Disconnect()
}

func BenchmarkQueueConnectDisconnect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q, _ := QueueConnect(":6379", "rq_test_queue")
		q.Disconnect()
	}
}

func BenchmarkQueuePush(b *testing.B) {
	q, _ := QueueConnect(":6379", "rq_test_queue_pushpop_bench")
	for i := 0; i < b.N; i++ {
		q.Push("foo")
	}
	q.Disconnect()
}

func BenchmarkQueueLength(b *testing.B) {
	q, _ := QueueConnect(":6379", "rq_test_queue_length_bench")
	q.Push("foo")
	for i := 0; i < b.N; i++ {
		q.Length()
	}
	q.Pop(1)
	q.Disconnect()
}
