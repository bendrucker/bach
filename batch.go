package bach

import (
	"time"
)

type ChannelBuffer struct {
	Size     int
	Interval time.Duration

	buffer *[]interface{}
	timer  *time.Timer
}

func NewBatch(inputs <-chan interface{}, size int, interval time.Duration) <-chan []interface{} {
	cb := ChannelBuffer{
		Size:     size,
		Interval: interval,

		timer:  time.NewTimer(interval),
		buffer: buffer(),
	}

	batches := make(chan []interface{})

	go cb.receive(inputs, batches)

	return batches
}

func (cb *ChannelBuffer) Flush(batches chan<- []interface{}) {
	if !cb.Empty() {
		batches <- *cb.buffer
		cb.Zero()
	}

	cb.timer.Reset(cb.Interval)
}

func (cb *ChannelBuffer) Zero() {
	cb.buffer = buffer()
}

func (cb *ChannelBuffer) Empty() bool {
	return len(*cb.buffer) == 0
}

func (cb *ChannelBuffer) Full() bool {
	return len(*cb.buffer) == cb.Size
}

func (cb *ChannelBuffer) Done(batches chan<- []interface{}) {
	cb.Flush(batches)
	close(batches)
	cb.timer.Stop()
}

func (cb *ChannelBuffer) receive(inputs <-chan interface{}, batches chan<- []interface{}) {
	for {
		select {
		case <-cb.timer.C:
			cb.Flush(batches)
		case item, ok := <-inputs:
			if !ok {
				cb.Done(batches)
				return
			}

			buffer := append(*cb.buffer, item)
			cb.buffer = &buffer

			if cb.Full() {
				cb.Flush(batches)
			}
		}
	}
}

func buffer() *[]interface{} {
	b := make([]interface{}, 0)
	return &b
}
