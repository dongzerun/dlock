package dlock

import (
	"errors"
	"fmt"
	"time"

	"github.com/dongzerun/nodemgr"
	"github.com/garyburd/redigo/redis"
)

var (
	// get取到key后，检查key是否存在
	// 1. 不存在，直接设置 key value expire 返回 0
	// 2. 存在，但是 token 相等说明己经锁过了 更新ttl 返回 1
	// 3. 存在，但是 token 不是自己的，抢锁失败 返回 2
	// 第一个参数是 key, 第二个是 value, 第三个是 expire
	LuaLock = "local token=redis.call('get', KEYS[1]) if(token) then if(token == ARGV[1]) then redis.call('setex', KEYS[1], ARGV[2], ARGV[1]) return 1 else return 2 end else redis.call('setex', KEYS[1], ARGV[2], ARGV[1]) return 0 end"
	// 解锁同理，不存在返回0，存在是自己的del后返回 1，不是自己的直接返回 2
	LuaUnLock = "local token=redis.call('get', KEYS[1]) if(token) then if(token == ARGV[1]) then redis.call('del', KEYS[1]) return 1 else return 2 end else return 0 end"
	// 强制解锁，同普通解锁，不过 不是自己的也要 del 后返回 2
	LuaUnLockForce = "local token=redis.call('get', KEYS[1]) if(token) then if(token == ARGV[1]) then redis.call('del', KEYS[1]) return 1 else redis.call('del', KEYS[1]) return 2 end else return 0 end"
)

var (
	ErrConnectRedis = errors.New("connect redis failed")
	ErrHostEmpty    = errors.New("redis host empty")
)

type RedisClient struct {
	name      string
	timeoutMs int
	passwd    string
}

func NewRedisClient(hosts []string, timeout int) (*RedisClient, error) {
	if len(hosts) == 0 {
		return nil, ErrHostEmpty
	}

	sc := nodemgr.NewServiceConf()
	sc.AddHosts(hosts...)

	cj := nodemgr.NewConfigJson()
	cj.AddService("dlock", sc)

	client := &RedisClient{
		timeoutMs: timeout,
		name:      "dlock",
	}

	err := nodemgr.InitWithConfig(cj)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (c *RedisClient) GetConn() (redis.Conn, error) {
	var (
		conn      redis.Conn
		err       error
		blackNode string
		host      string
	)

	for i := 0; i <= 1; i++ {
		host, err = nodemgr.GetNode(c.name, blackNode)
		if err != nil || host == "" {
			continue
		}

		conn, err = redis.DialTimeout("tcp", host,
			//服务连接超时、读写超时设置，单位毫秒
			time.Duration(c.timeoutMs)*time.Millisecond,
			time.Duration(c.timeoutMs)*time.Millisecond,
			time.Duration(c.timeoutMs)*time.Millisecond)
		if err != nil {
			blackNode = host
			nodemgr.Vote(c.name, host, nodemgr.UNHEALTHY)
			continue
		}
		nodemgr.Vote(c.name, host, nodemgr.HEALTHY)

		//支持密码认证，认证错误直接返回不重试
		if len(c.passwd) > 0 {
			if _, err := conn.Do("AUTH", c.passwd); err != nil {
				conn.Close()
				return nil, err
			}
		}

		return conn, nil
	}

	return nil, ErrConnectRedis
}

// redis 操作底层数据，需要 支持lua 脚本
type RedisDelegater struct {
	*RedisClient
}

func NewRedisDelegater(hosts []string, timeout int) (Delegater, error) {
	r, err := NewRedisClient(hosts, timeout)
	if err != nil {
		return nil, err
	}

	return &RedisDelegater{r}, nil
}

func (r *RedisDelegater) LockWithToken(key, value string, duration int) error {
	conn, err := r.RedisClient.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	ret, err := redis.Int(conn.Do("EVAL", LuaLock, 1, key, value, duration))
	if err != nil {
		return fmt.Errorf("%s %v", LuaLock, err)
	}

	switch ret {
	case 0, 1:
		return nil
	case 2:
		return ErrLockFailed
	}
	return ErrUnknown
}

func (r *RedisDelegater) UnLockWithToken(key, value string, force bool) error {
	conn, err := r.RedisClient.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	var ret int
	if force {
		ret, err = redis.Int(conn.Do("EVAL", LuaUnLockForce, 1, key, value))
	} else {
		ret, err = redis.Int(conn.Do("EVAL", LuaUnLock, 1, key, value))
	}
	if err != nil {
		return err
	}

	// 强制解锁不理会返回值 ret
	if force {
		return nil
	}

	switch ret {
	case 0, 1:
		return nil
	case 2:
		return ErrUnLockFailed
	}
	return ErrUnknown
}
