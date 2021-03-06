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
	"strings"
	"time"
)

func NewPool(connectString string, maxIdle int, maxActive int, idleTime time.Duration) *redis.Pool {
	pool := redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTime,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			urlParts := strings.Split(connectString, "/")
			c, err := redis.Dial("tcp", urlParts[0])
			if err != nil {
				return nil, err
			}

			if len(urlParts) == 2 {
				selectErr := c.Send("SELECT", urlParts[1])
				if selectErr != nil {
					return nil, selectErr
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return &pool
}
