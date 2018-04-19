package dlock

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// redis: 127.0.0.1:6379
func Test_LockAndUnlock(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis([]string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.SetKey(randKey).SetToken(randValue).SetDuration(expire).Lock()
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.UnLock(false)
	if err != nil {
		t.Fatalf("dlock unlock:%s token:%s err:%v", dlock.key, dlock.token, err)
	}
}

// redis: 127.0.0.1:6379
func Test_LockTwice(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis([]string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.SetKey(randKey).SetToken(randValue).SetDuration(expire).Lock()
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.Lock()
	if err != nil {
		t.Fatalf("dlock twice set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

}

func Test_UnlockEmpty(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))

	dlock, err := NewDLockerWithRedis([]string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.SetKey(randKey).SetToken(randValue).UnLock(false)
	if err != nil {
		t.Fatalf("dlock unlock:%s token:%s err:%v", dlock.key, dlock.token, err)
	}
}

func Test_UnLockOther(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	otherValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis([]string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.SetKey(randKey).SetToken(randValue).SetDuration(expire).Lock()
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.SetToken(otherValue).UnLock(false)
	if err == nil {
		t.Fatalf("dlock unlock:%s token:%s %v success, expected failed", dlock.key, dlock.token, err)
	}

	err = dlock.SetToken(otherValue).UnLock(true)
	if err != nil {
		t.Fatalf("dlock force unlock:%s token:%s err:%v", dlock.key, dlock.token, err)
	}
}
