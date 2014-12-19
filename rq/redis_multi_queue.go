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
	"math"
	"math/rand"
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
func MultiQueueConnect(pool []*redis.Pool, key string) (*MultiQueue, error) {
	queues := []*ErrorDecayQueue{}
	for _, pooledConnection := range pool {
		queue := &ErrorDecayQueue{
			pooledConnection: pooledConnection,
			errorRatingTime:  time.Now().Unix(),
			errorRating:      0.0,
		}
		queues = append(queues, queue)
	}
	return &MultiQueue{key: key, queues: queues}, nil
}

// Push will perform a left-push onto a Redis list/queue with the supplied
// key and value.  An error will be returned if the operation failed.
func (multi_queue *MultiQueue) Push(value string) error {
	q, err := multi_queue.SelectHealthyQueue()
	if err != nil {
		return err
	}

	conn := q.pooledConnection.Get()
	defer conn.Close()

	_, err = conn.Do("LPUSH", multi_queue.key, value)
	if err != nil && err != redis.ErrNil {
		q.QueueError()
	}
	return err
}

// Pop will perform a blocking right-pop from a Redis list/queue with the supplied
// key.  An error will be returned if the operation failed.
func (multi_queue *MultiQueue) Pop(timeout int) (string, error) {
	q, err := multi_queue.SelectHealthyQueue()
	if err != nil {
		return "", err
	}

	conn := q.pooledConnection.Get()
	defer conn.Close()

	r, err := redis.Strings(conn.Do("BRPOP", multi_queue.key, timeout))
	if err == nil {
		return r[1], nil
	} else {
		if err != redis.ErrNil {
			q.QueueError()
		}
		return "", err
	}
}

// Length will return the number of items in the specified list/queue
func (multi_queue *MultiQueue) Length() (int, error) {
	count := 0
	for _, q := range multi_queue.HealthyQueues() {
		conn := q.pooledConnection.Get()
		defer conn.Close()

		rep, err := redis.Int(conn.Do("LLEN", multi_queue.key))
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
	for _, q := range mq.queues {
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
