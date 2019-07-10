package bach

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ExampleNewBatcher() {
	numbers := make(chan interface{}, 10)
	batcher := NewBatcher(numbers, BatchLimits{10, time.Duration(100)})

	numbers <- 1
	numbers <- 2
	numbers <- 3

	// triggers close of batcher channel
	close(numbers)

	for batch := range batcher.Results() {
		fmt.Println(batch)
	}

	// Output: [1 2 3]
}

func TestLimitSize(t *testing.T) {
	batch := NewBatcher(alphabet(), BatchLimits{13, time.Duration(1) * time.Hour})

	first := <-batch.Results()
	second := <-batch.Results()

	assert.Len(t, first, 13)
	assert.Len(t, second, 13)

	assert.EqualValues(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"}, stringSlice(first))
	assert.EqualValues(t, []string{"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}, stringSlice(second))
}

func TestLimitAge(t *testing.T) {
	ch := make(chan interface{}, 10)
	batcher := NewBatcher(ch, BatchLimits{10, time.Duration(20) * time.Millisecond})
	done := make(chan bool, 1)

	ch <- 1
	go func() {
		// sleep 1.5x age limit to allow flush
		time.Sleep(time.Duration(30) * time.Millisecond)
		assert.Equal(t, 0, batcher.Len())

		// sleep < age limit
		// verify that timer began on receipt of 2, not on flush of 1
		ch <- 2
		time.Sleep(time.Duration(5) * time.Millisecond)
		assert.Equal(t, 1, batcher.Len())

		done <- true
	}()

	first := <-batcher.Results()
	second := <-batcher.Results()

	assert.Len(t, first, 1)
	assert.Len(t, second, 1)

	assert.Equal(t, 1, first[0].(int))
	assert.Equal(t, 2, second[0].(int))

	<-done
}

func alphabet() <-chan interface{} {
	length := 26

	ch := make(chan interface{}, length)
	defer close(ch)

	for i := 0; i < length; i++ {
		ch <- string('a' + byte(i))
	}

	return ch
}

func stringSlice(slice []interface{}) []string {
	strings := make([]string, len(slice))
	for i := range slice {
		strings[i] = slice[i].(string)
	}
	return strings
}
