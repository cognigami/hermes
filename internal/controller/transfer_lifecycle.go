package controller

import (
	"context"
	"errors"
	"sync"
)

// Transfers are Go handlers, not OS children. Authority close first prevents
// new handlers, cancels their IO, and joins them before releasing the root lock.
func (c *Controller) beginArtifactTransfer(ctx context.Context) (context.Context, func(), error) {
	c.transferMu.Lock()
	if c.closing {
		c.transferMu.Unlock()
		return nil, nil, errors.New("controller authority closing")
	}
	c.transferWG.Add(1)
	c.transferMu.Unlock()
	derived, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifetime, cancel)
	var once sync.Once
	finish := func() { once.Do(func() { stop(); cancel(); c.transferWG.Done() }) }
	return derived, finish, nil
}
