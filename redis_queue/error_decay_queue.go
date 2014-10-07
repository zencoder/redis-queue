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

import "math"
import "math/rand"
import "time"
import redis "github.com/garyburd/redigo/redis"

type ErrorDecayQueue struct {
	conn redis.Conn
	address string
	error_rating float64
	error_rating_time int64
}

type MultiQueue struct {
	key string
	queues []*ErrorDecayQueue
}

func HealthyQueues (queues *[]*ErrorDecayQueue) ([]*ErrorDecayQueue) {
	now := time.Now().Unix()
	healthy_queues := []*ErrorDecayQueue{}
	for _, queue := range *queues {
		time_delta := now - queue.error_rating_time
		updated_error_rating := queue.error_rating * math.Exp((math.Log(0.5) / 10) * float64(time_delta))

		if updated_error_rating < 0.1 {
			if queue.error_rating >= 0.1 {
				// transitioning the queue out of an unhealthy state, try reconnecting
				queue.conn.Close()
				conn, error := redis.Dial("tcp", queue.address)
				if error == nil {
					queue.conn = conn
					healthy_queues = append(healthy_queues, queue)
				}
			} else {
				healthy_queues = append(healthy_queues, queue)
			}
		}
		queue.error_rating_time = time.Now().Unix()
		queue.error_rating = updated_error_rating
	}
	return healthy_queues
}

func SelectHealthyQueue(multi_queue *MultiQueue) (*ErrorDecayQueue, error) {
	healthy_queues := HealthyQueues(&multi_queue.queues)
	number_of_healthy_queues := len(healthy_queues)

	index := 0
	if number_of_healthy_queues == 0 {
		number_of_queues := len(multi_queue.queues)
		index = rand.Intn(number_of_queues)
		return multi_queue.queues[index], nil
	} else if number_of_healthy_queues > 1 {
		index = rand.Intn(number_of_healthy_queues)
	}
	return healthy_queues[index], nil
}

func QueueError(queue *ErrorDecayQueue) {
	queue.error_rating = queue.error_rating + 0.1
}
