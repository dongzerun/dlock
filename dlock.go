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

// 分布式锁 线程安全
type DLocker struct {
	Delegater
	// token    string // 唯一标识
	// key      string // 锁定 key
	// duration int    // 锁定时间 单位秒
}

func (dl *DLocker) Lock(key, token string, duration int) error {
	ekey := fmt.Sprintf("%s_%s", KeyPrefix, key)
	return dl.Delegater.LockWithToken(ekey, token, duration)
}

func (dl *DLocker) UnLock(key, token string, force bool) error {
	ekey := fmt.Sprintf("%s_%s", KeyPrefix, key)
	return dl.Delegater.UnLockWithToken(ekey, token, force)
}

func NewDLockerWithRedis(name string, hosts []string) (*DLocker, error) {
	return NewDLockerWithRedisTimeoutMs(name, hosts, DefaultTimeOut)
}

func NewDLockerWithRedisTimeoutMs(name string, hosts []string, timeout int) (*DLocker, error) {
	delegater, err := NewRedisDelegater(name, hosts, timeout)
	if err != nil {
		return nil, err
	}

	dl := &DLocker{
		Delegater: delegater,
	}
	return dl, nil
}
