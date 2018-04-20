package dlock

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func Test_WriteRedisOK(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	d, err := NewRedisDelegater("testlock", []string{"127.0.0.1:6379"}, 100)
	if err != nil {
		t.Fatalf("NewRedisDelegater err:%v", err)
	}

	err = d.LockWithToken(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("LockWithToken err:%v", err)
	}

	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Fatalf("dial redis 127.0.0.1:6379 %v", err)
	}

	value, err := redis.String(conn.Do("get", randKey))
	if err != nil {
		t.Fatalf("redis get %s err:%v", randKey, err)
	}

	if value != randValue {
		t.Fatalf("redis get value:%s expected:%s", value, randValue)
	}

	ttl, err := redis.Int(conn.Do("ttl", randKey))
	if err != nil {
		t.Fatalf("redis ttl %s err:%v", randKey, err)
	}

	if ttl != expire {
		t.Fatalf("redis ttl get %d, but expected %d", ttl, expire)
	}
}
