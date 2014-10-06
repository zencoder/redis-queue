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

import "errors"
import "math"
import "math/rand"
import "time"
import redis "github.com/garyburd/redigo/redis"

type ErrorDecayQueue struct {
	conn redis.Conn
	error_rating float64
	error_rating_time int64
}

type MultiQueue struct {
	key string
	queues []ErrorDecayQueue
}

func HealthyQueues (multi_queue *MultiQueue) ([]ErrorDecayQueue) {
	now := time.Now().Unix()
	healthy_queues := []ErrorDecayQueue{}
	for _, queue := range multi_queue.queues {
		time_delta := now - queue.error_rating_time
		queue.error_rating_time = now
		queue.error_rating = queue.error_rating * math.Exp((math.Log(0.5) / 10) * float64(time_delta))
		if queue.error_rating < 0.1 {
			healthy_queues = append(healthy_queues, queue)
		}
	}
	return healthy_queues
}

func SelectHealthyQueue(multi_queue *MultiQueue) (*ErrorDecayQueue, error) {
	healthy_queues := HealthyQueues(multi_queue)
	number_of_healthy_queues := len(healthy_queues)

	if number_of_healthy_queues == 0 {
		return &ErrorDecayQueue{}, errors.New("Unable to find a healthy queue to send to")
	} else if number_of_healthy_queues == 1 {
		return &healthy_queues[0], nil
	} else {
		return &healthy_queues[RandomInRange(0, number_of_healthy_queues - 1)], nil
	}
}

func RandomInRange(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max - min) + min
}

func QueueError(queue *ErrorDecayQueue) {
	queue.error_rating = queue.error_rating + 0.1
}
