/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

// Package redis_queue provides a simple queue abstraction that is backed by Redis.
package redis_queue

import redis "github.com/garyburd/redigo/redis"

// Connect opens a connection to a Redis server and returns the connection.
// The connection should be closed by invoking Disconnect(conn),
// likely with defer.
func Connect(address string) (redis.Conn, error) {
	return redis.Dial("tcp", address)
}

// Close the Redis connection
func Disconnect(conn redis.Conn) {
	conn.Close()
}

// Push will perform a right-push onto a Redis list/queue with the supplied 
// key and value.  An error will be returned if the operation failed.
func Push(conn redis.Conn, key string, value string) (error) {
	err := conn.Send("RPUSH", key, value)
	if err == nil {
		return conn.Flush()
	} else {
		return err
	}
}

// Pop will perform a blocking left-pop from a Redis list/queue with the supplied 
// key.  An error will be returned if the operation failed.
func Pop(conn redis.Conn, key string, timeout int) (string, error) {
	rep, err := redis.Strings(conn.Do("BLPOP", key, timeout))
	if err == nil {
		return rep[1], nil
	} else {
		return "", err
	}
}

// Length will return the number of items in the specified list/queue
func Length(conn redis.Conn, key string) (int, error) {
	rep, err := redis.Int(conn.Do("LLEN", key))
	if err == nil {
		return rep, nil
	} else {
		return 0, err
	}
}
