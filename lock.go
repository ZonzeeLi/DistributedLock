package DistributedLock

import (
	"context"
	_ "embed"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	ErrLockAlreadyAcquired = errors.New("lock already acquired")
	ErrLockNotHold         = errors.New("lock not hold")
)

var (
	//go:embed unlock.lua
	luaUnlock string
	//go:embed refresh.lua
	luaRefresh string
	//go:embed lock.lua
	luaLock string
)

type Client struct {
	client redis.Cmdable
	s      singleflight.Group
}

// NewClient creates a new client.
func NewClient(client redis.Cmdable) *Client {
	return &Client{client: client}
}

// TryLock tries to acquire a lock.
func (c *Client) TryLock(ctx context.Context, key string, expire time.Duration) (*Lock, error) {
	uv := uuid.New().String()
	res, err := c.client.SetNX(ctx, key, uv, expire).Result()
	if err != nil {
		return nil, err
	}
	if !res {
		return nil, ErrLockAlreadyAcquired
	}
	return newLock(c.client, key, uv, expire), nil
}

// Lock acquires a lock.
func (c *Client) Lock(ctx context.Context, key string, expire time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	uv := uuid.New().String()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		lctx, cancel := context.WithTimeout(ctx, timeout)
		res, err := c.client.Eval(lctx, luaLock, []string{key}, uv, expire.Milliseconds()).Bool()
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if res {
			return newLock(c.client, key, uv, expire), nil
		}
		interval, ok := retry.Next()
		if !ok {
			if err != nil {
				return nil, err
			} else {
				return nil, ErrLockNotHold
			}
		}
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) SingleFlightLock(ctx context.Context, key string, expire time.Duration, retry RetryStrategy, timeout time.Duration) (*Lock, error) {
	for {
		flag := false
		resCh := c.s.DoChan(key, func() (interface{}, error) {
			flag = true
			return c.Lock(ctx, key, expire, retry, timeout)
		})
		select {
		case res := <-resCh:
			if flag {
				c.s.Forget(key)
				if res.Err != nil {
					return nil, res.Err
				}
				return res.Val.(*Lock), nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Lock is a distributed lock.
type Lock struct {
	client           redis.Cmdable
	key              string
	uv               string
	expire           time.Duration
	unlock           chan struct{}
	signalUnlockOnce sync.Once
}

// newLock creates a new lock.
func newLock(client redis.Cmdable, key string, uv string, expire time.Duration) *Lock {
	return &Lock{client: client, key: key, uv: uv, expire: expire, unlock: make(chan struct{}, 1)}
}

// Unlock unlocks the lock.
func (l *Lock) Unlock(ctx context.Context) error {
	defer func() {
		l.unlock <- struct{}{}
		close(l.unlock)
	}()
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.uv).Int64()
	if err == redis.Nil {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// Refresh refreshes the lock.
func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.uv, l.expire.Milliseconds()).Result()
	if err == redis.Nil {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// AutoRefresh refreshes the lock automatically.
func (l *Lock) AutoRefresh(interval time.Duration, timeout time.Duration) error {
	ch := make(chan struct{}, 1)
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		close(ch)
	}()
	for {
		select {
		case <-ch:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancel()
			if err == context.DeadlineExceeded {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancel()
			if err == context.DeadlineExceeded {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-l.unlock:
			return nil
		}
	}
}
