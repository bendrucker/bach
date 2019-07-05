package bach

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ExampleNewBatch() {
	numbers := make(chan interface{}, 10)
	batches := NewBatch(numbers, 10, time.Duration(100))

	numbers <- 1
	numbers <- 2
	numbers <- 3
	close(numbers)

	for batch := range batches {
		fmt.Println(batch)
	}

	// batch is closed when numbers is closed
	// Output: [1, 2, 3]
}

func TestNewBatchFullBuffer(t *testing.T) {
	batches := NewBatch(alphabet(), 13, time.Duration(1)*time.Hour)

	first := <-batches
	second := <-batches

	assert.Len(t, first, 13)
	assert.Len(t, second, 13)

	assert.EqualValues(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"}, stringSlice(first))
	assert.EqualValues(t, []string{"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}, stringSlice(second))
}

func TestNewBatchInterval(t *testing.T) {
	ch := make(chan interface{}, 10)
	ch <- 1
	go func() {
		time.Sleep(time.Duration(200))
		ch <- 2
	}()

	batches := NewBatch(ch, 10, time.Duration(100))

	first := <-batches
	second := <-batches

	assert.Len(t, first, 1)
	assert.Len(t, second, 1)

	assert.Equal(t, 1, first[0].(int))
	assert.Equal(t, 2, second[0].(int))
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
