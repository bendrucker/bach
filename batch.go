// Package bach provides configurable batching for messages on a user-supplied channel.
// It supports both timer and size based batching criteria.
package bach

import (
	"sync"
	"time"
)

// NewBatcher initializes a new ChannelBatcher and begins reading from the inputs channel
func NewBatcher(inputs <-chan interface{}, limits BatchLimits) *ChannelBatcher {
	cb := &ChannelBatcher{
		Limits:  limits,
		batches: make(chan []interface{}),
		buffer:  buffer(limits.Size),
		mutex:   &sync.RWMutex{},
		timer:   time.NewTimer(limits.Age),
	}

	cb.timer.Stop()
	go cb.run(inputs)

	return cb
}

// BatchLimits configures limits for the contents of a batch
// When limits are reached, a batch will be flushed
type BatchLimits struct {
	// Size is the maximum size of a batch. When the buffer reaches Size, a batch
	// will be flushed.
	Size int

	// Age is the maximum age (a Duration) of an entry in a batch. When the first
	// entry in the buffer reaches Age, a batch will be flushed.
	Age time.Duration
}

// ChannelBatcher stores the state of the channel batching operation.
// It records configuration (Size and Interval) and maintains the buffer and timer.
type ChannelBatcher struct {
	Limits BatchLimits

	batches chan []interface{}
	buffer  []interface{}
	index   int
	mutex   *sync.RWMutex
	timer   *time.Timer
}

// Results returns a read-only batch channel that will receive arrays of interfaces (batches)
func (cb *ChannelBatcher) Results() <-chan []interface{} {
	return cb.batches
}

// Flush writes a the current buffer slice to the batch output channel.
// It zeroes the buffer and resets the timer.
func (cb *ChannelBatcher) Flush() {
	if !cb.Empty() {
		cb.batches <- cb.drain()
	}
}

// Len returns the current number of entries in the buffer
func (cb *ChannelBatcher) Len() int {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.index
}

// Drain returns a slice sized based on the number of buffered items containing the buffered values.
// It also zeroes the buffer.
func (cb *ChannelBatcher) drain() []interface{} {
	result := make([]interface{}, cb.Len())

	cb.mutex.RLock()
	for i := 0; i < cb.index; i++ {
		result[i] = cb.buffer[i]
	}
	cb.mutex.RUnlock()

	cb.zero()

	return result
}

// zero sets the buffer pointer to a new empty buffer
func (cb *ChannelBatcher) zero() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.buffer = buffer(cb.Limits.Size)
	cb.index = 0
}

// Empty checks whether the buffer is empty
func (cb *ChannelBatcher) Empty() bool {
	return cb.Len() == 0
}

// Full checks whether the buffer is full and needs to be flushed
func (cb *ChannelBatcher) Full() bool {
	return cb.Len() == cb.Limits.Size
}

// Done flushes any buffered values and closes the output channel. It also stops the flush timer.
func (cb *ChannelBatcher) Done() {
	cb.Flush()
	close(cb.batches)
	cb.timer.Stop()
}

func (cb *ChannelBatcher) append(item interface{}) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.buffer[cb.index] = item
	cb.index++
}

// run is the processing loop that handles timer expiration and new values on the input channel
func (cb *ChannelBatcher) run(inputs <-chan interface{}) {
	for {
		select {
		case <-cb.timer.C:
			cb.Flush()
		case item, ok := <-inputs:
			if !ok {
				cb.Done()
				return
			}

			if cb.Empty() {
				cb.timer.Reset(cb.Limits.Age)
			}

			cb.append(item)

			if cb.Full() {
				cb.Flush()
			}
		}
	}
}

func buffer(size int) []interface{} {
	return make([]interface{}, size)
}
