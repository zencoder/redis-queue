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
	"math"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type ErrorDecayQueue struct {
	server           string
	queueName        string
	pooledConnection *redis.Pool
	errorRating      float64
	errorRatingTime  int64

	mu sync.Mutex
}

func NewErrorDecayQueue(server string, queueName string, pooledConnection *redis.Pool) *ErrorDecayQueue {
	return &ErrorDecayQueue{
		server:           server,
		queueName:        queueName,
		pooledConnection: pooledConnection,
		errorRatingTime:  time.Now().Unix(),
		errorRating:      0.0,
	}
}

func (e *ErrorDecayQueue) QueueError() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.errorRating = e.errorRating + 0.1
}

func (e *ErrorDecayQueue) IsHealthy() (healthy bool) {
	healthy = false
	now := time.Now().Unix()
	timeDelta := now - e.errorRatingTime
	updatedErrorRating := e.errorRating * math.Exp((math.Log(0.5)/10)*float64(timeDelta))

	if e.errorRating < 0.1 {
		healthy = true
	} else {
		if updatedErrorRating < 0.1 {
			// transitioning the queue out of an unhealthy state, try issuing a ping
			conn := e.pooledConnection.Get()
			defer conn.Close()

			if _, err := conn.Do("PING"); err == nil {
				healthy = true
			}
		}
	}
	e.errorRatingTime = now
	e.errorRating = updatedErrorRating
	return
}
