package dlock

import (
	"errors"
	"fmt"
)

var (
	KeyPrefix = "dl"

	DefaultTimeOut = 100 // 100ms

	ErrLockFailed   = errors.New("dlock failed")
	ErrUnLockFailed = errors.New("dlock unlock failed")
	ErrUnknown      = errors.New("dlock unexpected error")
)

// 代理接口，后端可以自定义实现接口即可
type Delegater interface {
	LockWithToken(key, value string, duration int) error
	UnLockWithToken(key, value string, force bool) error
}

// 分布式锁, 非线程安全
type DLocker struct {
	Delegater
	token    string // 唯一标识
	key      string // 锁定 key
	duration int    // 锁定时间 单位秒
}

func (dl *DLocker) SetToken(t string) *DLocker {
	dl.token = t
	return dl
}

func (dl *DLocker) SetKey(k string) *DLocker {
	dl.key = k
	return dl
}

func (dl *DLocker) SetDuration(d int) *DLocker {
	if d < 0 {
		panic("dlocker duration must not negative")
	}

	dl.duration = d
	return dl
}

func (dl *DLocker) Lock() error {
	key := fmt.Sprintf("%s_%s", KeyPrefix, dl.key)
	return dl.Delegater.LockWithToken(key, dl.token, dl.duration)
}

func (dl *DLocker) UnLock(force bool) error {
	key := fmt.Sprintf("%s_%s", KeyPrefix, dl.key)
	return dl.Delegater.UnLockWithToken(key, dl.token, force)
}

func NewDLockerWithRedis(hosts []string) (*DLocker, error) {
	return NewDLockerWithRedisTimeoutMs(hosts, DefaultTimeOut)
}

func NewDLockerWithRedisTimeoutMs(hosts []string, timeout int) (*DLocker, error) {
	delegater, err := NewRedisDelegater(hosts, timeout)
	if err != nil {
		return nil, err
	}

	dl := &DLocker{
		Delegater: delegater,
	}
	return dl, nil
}
