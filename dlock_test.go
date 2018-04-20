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

	dlock, err := NewDLockerWithRedis("testlock", []string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.Lock(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.UnLock(randKey, randValue, false)
	if err != nil {
		t.Fatalf("dlock unlock:%s token:%s err:%v", randKey, randValue, err)
	}
}

// redis: 127.0.0.1:6379
func Test_LockTwice(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis("testlock", []string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.Lock(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.Lock(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("dlock twice set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

}

func Test_LockFailed(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis("testlock", []string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.Lock(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	newValue := randValue + "111"
	err = dlock.Lock(randKey, newValue, expire)
	if err == nil {
		t.Fatalf("dlock  set:%s token:%s duration:%d success, but expected failed:%v", randKey, randValue, expire, err)
	}

}

func Test_UnlockEmpty(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))

	dlock, err := NewDLockerWithRedis("testlock", []string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.UnLock(randKey, randValue, false)
	if err != nil {
		t.Fatalf("dlock unlock:%s token:%s err:%v", randKey, randValue, err)
	}
}

func Test_UnLockOther(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randKey := strconv.Itoa(rand.Intn(1000000))
	randValue := strconv.Itoa(rand.Intn(1000000))
	otherValue := strconv.Itoa(rand.Intn(1000000))
	expire := 1000

	dlock, err := NewDLockerWithRedis("testlock", []string{"127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("NewDlockerWithRedis err:%v", err)
	}

	err = dlock.Lock(randKey, randValue, expire)
	if err != nil {
		t.Fatalf("dlock set:%s token:%s duration:%d err:%v", randKey, randValue, expire, err)
	}

	err = dlock.UnLock(randKey, otherValue, false)
	if err == nil {
		t.Fatalf("dlock unlock:%s token:%s %v success, expected failed", randKey, otherValue, err)
	}

	err = dlock.UnLock(randKey, otherValue, true)
	if err != nil {
		t.Fatalf("dlock force unlock:%s token:%s err:%v", randKey, otherValue, err)
	}
}
