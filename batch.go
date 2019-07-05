package bach

import (
	"time"
)

// NewBatch accepts an input channel, buffer size, and flush interval.
// It creates a ChannelBatch and runs its receive loop in a new goroutine.
func NewBatch(inputs <-chan interface{}, size int, interval time.Duration) <-chan []interface{} {
	cb := ChannelBatch{
		Size:     size,
		Interval: interval,

		timer:  time.NewTimer(interval),
		buffer: buffer(size),
	}

	batches := make(chan []interface{})

	go cb.run(inputs, batches)

	return batches
}

// ChannelBatch stores a fixed-size buffer and a timer
type ChannelBatch struct {
	Size     int
	Interval time.Duration

	buffer []interface{}
	index  int
	timer  *time.Timer
}

// Flush writes a the current buffer slice to the batch output channel.
// It zeroes the buffer and resets the timer.
func (cb *ChannelBatch) Flush(batches chan<- []interface{}) {
	if !cb.Empty() {
		batches <- cb.Drain()

	}

	cb.timer.Reset(cb.Interval)
}

// Drain returns a slice sized based on the number of buffered items containing the buffered values.
// It also zeroes the buffer.
func (cb *ChannelBatch) Drain() []interface{} {
	result := make([]interface{}, cb.index)
	for i := 0; i < cb.index; i++ {
		result[i] = cb.buffer[i]
	}
	cb.Zero()
	return result
}

// Zero sets the buffer pointer to a new empty buffer
func (cb *ChannelBatch) Zero() {
	cb.buffer = buffer(cb.Size)
	cb.index = 0
}

// Empty checks whether the buffer is empty
func (cb *ChannelBatch) Empty() bool {
	return cb.index == 0
}

// Full checks whether the buffer is full and needs to be flushed
func (cb *ChannelBatch) Full() bool {
	return cb.index == cb.Size
}

// Done flushes any buffered values and closes the output channel. It also stops the flush timer.
func (cb *ChannelBatch) Done(batches chan<- []interface{}) {
	cb.Flush(batches)
	close(batches)
	cb.timer.Stop()
}

// run is the processing loop that handles timer expiration and new values on the input channel
func (cb *ChannelBatch) run(inputs <-chan interface{}, batches chan<- []interface{}) {
	for {
		select {
		case <-cb.timer.C:
			cb.Flush(batches)
		case item, ok := <-inputs:
			if !ok {
				cb.Done(batches)
				return
			}

			cb.buffer[cb.index] = item
			cb.index++

			if cb.Full() {
				cb.Flush(batches)
			}
		}
	}
}

func buffer(size int) []interface{} {
	return make([]interface{}, size)
}
