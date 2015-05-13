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
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type MultiQueue struct {
	mu     sync.Mutex
	key    string
	pools  []*redis.Pool
	queues []*ErrorDecayQueue
}

var noQueuesAvailableError = errors.New("No queues available")

func NewMultiQueue(pools []*redis.Pool, key string) *MultiQueue {
	queues := []*ErrorDecayQueue{}
	for _, pooledConnection := range pools {
		queue := &ErrorDecayQueue{
			pooledConnection: pooledConnection,
			errorRatingTime:  time.Now().Unix(),
			errorRating:      0.0,
		}
		queues = append(queues, queue)
	}
	return &MultiQueue{key: key, pools: pools, queues: queues}
}

// Push will perform a left-push onto a Redis list/queue with the supplied
// key and value.  An error will be returned if the operation failed.
func (m *MultiQueue) Push(value string) (err error) {
	var q *ErrorDecayQueue
	if q, err = m.SelectHealthyQueue(); err != nil {
		return err
	}

	if q == nil {
		log.Println("selected queue is nil")
	}
	if q.pooledConnection == nil {
		log.Println("selected connection is nil")
	}
	conn := q.pooledConnection.Get()
	defer conn.Close()

	if _, err = conn.Do("LPUSH", m.key, value); err != nil && err != redis.ErrNil {
		// flag the queue as having encountered an error
		q.QueueError()
	}
	return
}

// Pop will perform a blocking right-pop from a Redis list/queue with the supplied
// key.  An error will be returned if the operation failed.
func (m *MultiQueue) Pop(timeout int) (message string, err error) {
	var q *ErrorDecayQueue
	if q, err = m.SelectHealthyQueue(); err != nil {
		return
	}

	conn := q.pooledConnection.Get()
	defer conn.Close()

	var r []string
	if r, err = redis.Strings(conn.Do("BRPOP", m.key, timeout)); err == nil {
		if len(r) > 0 {
			message = r[1]
		}
	} else {
		if err == redis.ErrNil {
			err = nil // clear out the error if it's just signaling no data was read
		}
	}
	return
}

// Length will return the number of items in the specified list/queue
func (m *MultiQueue) Length() (total int, err error) {
	total = 0
	for _, q := range m.HealthyQueues() {
		conn := q.pooledConnection.Get()
		defer conn.Close()

		var rep int
		if rep, err = redis.Int(conn.Do("LLEN", m.key)); err != nil {
			return
		}
		total = total + rep
	}
	return
}

func (m *MultiQueue) HealthyQueues() (healthyQueues []*ErrorDecayQueue) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().Unix()
	healthyQueues = make([]*ErrorDecayQueue, 0)
	for _, q := range m.queues {
		timeDelta := now - q.errorRatingTime
		updatedErrorRating := q.errorRating * math.Exp((math.Log(0.5)/10)*float64(timeDelta))

		if updatedErrorRating < 0.1 {
			if q.errorRating >= 0.1 {
				// transitioning the queue out of an unhealthy state, try issuing a ping
				conn := q.pooledConnection.Get()
				defer conn.Close()

				_, err := conn.Do("PING")
				if err == nil {
					healthyQueues = append(healthyQueues, q)
				}
			} else {
				healthyQueues = append(healthyQueues, q)
			}
		}
		q.errorRatingTime = time.Now().Unix()
		q.errorRating = updatedErrorRating
	}
	return
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
