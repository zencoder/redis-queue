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

func TestQueueDisconnectSuccessful(t *testing.T) {
	q, err := QueueConnect(":6379", "rq_test_queue")
	q.Disconnect()
	if err != nil {
		t.Error("Error while disconnecting from Redis", err)
	}
}

func BenchmarkQueueConnectDisconnect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q, _ := QueueConnect(":6379", "rq_test_queue")
		q.Disconnect()
	}
}

