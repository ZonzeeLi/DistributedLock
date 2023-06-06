package DistributedLock

import (
	"DistributedLock/mocks"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_TryLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		name string
		mock func() redis.Cmdable

		key    string
		expire time.Duration

		wantErr  error
		wantLock *Lock
	}{
		{
			name: "locked",
			mock: func() redis.Cmdable {
				cmdable := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(true, nil)
				cmdable.EXPECT().SetNX(gomock.Any(), "locked-key", gomock.Any(), time.Minute).Return(res)
				return cmdable
			},
			key:    "locked-key",
			expire: time.Minute,

			wantLock: &Lock{
				key: "locked-key",
			},
		},
		{
			name: "network error",
			mock: func() redis.Cmdable {
				cmdable := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, errors.New("network error"))
				cmdable.EXPECT().SetNX(gomock.Any(), "network-key", gomock.Any(), time.Minute).Return(res)
				return cmdable
			},
			key:    "network-key",
			expire: time.Minute,

			wantErr: errors.New("network error"),
		},
		{
			name: "failed locked",
			mock: func() redis.Cmdable {
				cmdable := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, nil)
				cmdable.EXPECT().SetNX(gomock.Any(), "failed-key", gomock.Any(), time.Minute).Return(res)
				return cmdable
			},
			key:    "failed-key",
			expire: time.Minute,

			wantErr: ErrLockAlreadyAcquired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(tc.mock())
			lock, err := client.TryLock(nil, tc.key, tc.expire)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.NotNil(t, lock.client)
			assert.Equal(t, tc.wantLock.key, lock.key)
			assert.NotEmpty(t, lock.uv)
		})
	}
}
