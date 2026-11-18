//go:build !linux

package orker

import (
	"context"
	"errors"
	"time"
)

var errGPULockLinuxOnly = errors.New("the GPU host lock needs a Linux host")

func openGPULock(string) (*gpuHostLock, error) { return nil, errGPULockLinuxOnly }

func (l *gpuHostLock) tryLock() (bool, error) { return false, errGPULockLinuxOnly }

func (l *gpuHostLock) wait(context.Context, time.Duration, func(lockHolder)) error {
	return errGPULockLinuxOnly
}

func (l *gpuHostLock) holder() lockHolder { return lockHolder{err: errGPULockLinuxOnly} }

func (l *gpuHostLock) check() error { return errGPULockLinuxOnly }

func (l *gpuHostLock) close() {}
