package DistributedLock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestClient_TryLock_e2e(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
	})
	client := NewClient(rdb)
	client.Wait()

	testCases := []struct {
		name string

		before func()
		after  func()

		key    string
		expire time.Duration

		wantErr  error
		wantLock *Lock
	}{
		{
			name:   "locked",
			before: func() {},
			after: func() {
				res, err := rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			key:    "locked-key",
			expire: time.Minute,

			wantLock: &Lock{
				key: "locked-key",
			},
		},
		{
			name: "failed locked",
			before: func() {
				val, err := rdb.SetNX(context.Background(), "failed-key", "1", time.Minute).Result()
				require.NoError(t, err)
				require.Equal(t, true, val)
			},
			after: func() {
				res, err := rdb.Get(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, "1", res)
			},
			key:    "failed-key",
			expire: time.Minute,

			wantErr: ErrLockAlreadyAcquired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			lock, err := client.TryLock(context.Background(), tc.key, tc.expire)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			tc.after()
			assert.NotNil(t, lock.client)
			assert.Equal(t, tc.wantLock.key, lock.key)
			assert.NotEmpty(t, lock.uv)
		})
	}
}

func (c *Client) Wait() {
	for c.client.Ping(context.Background()).Err() != nil {
	}
}
