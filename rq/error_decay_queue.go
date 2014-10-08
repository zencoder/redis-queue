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
import "math"
import "math/rand"
import "time"
import redis "github.com/garyburd/redigo/redis"

type ErrorDecayQueue struct {
	conn redis.Conn
	address string
	errorRating float64
	errorRatingTime int64
}

type MultiQueue struct {
	key string
	queues []*ErrorDecayQueue
}

func (mq *MultiQueue) HealthyQueues() ([]*ErrorDecayQueue) {
	now := time.Now().Unix()
	healthyQueues := []*ErrorDecayQueue{}
	for _, queue := range mq.queues {
		timeDelta := now - queue.errorRatingTime
		updatedErrorRating := queue.errorRating * math.Exp((math.Log(0.5) / 10) * float64(timeDelta))

		if updatedErrorRating < 0.1 {
			if queue.errorRating >= 0.1 {
				// transitioning the queue out of an unhealthy state, try reconnecting
				queue.conn.Close()
				conn, error := redis.Dial("tcp", queue.address)
				if error == nil {
					queue.conn = conn
					healthyQueues = append(healthyQueues, queue)
				}
			} else {
				healthyQueues = append(healthyQueues, queue)
			}
		}
		queue.errorRatingTime = time.Now().Unix()
		queue.errorRating = updatedErrorRating
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
			return nil, errors.New("No queues available")
		}
		index = rand.Intn(numberOfQueues)
		return mq.queues[index], nil
	} else if numberOfHealthyQueues > 1 {
		index = rand.Intn(numberOfHealthyQueues)
	}
	return healthyQueues[index], nil
}

func (queue *ErrorDecayQueue) QueueError() {
	queue.errorRating = queue.errorRating + 0.1
}
