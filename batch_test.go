package bach

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBatchFullBuffer(t *testing.T) {
	batches := NewBatch(alphabet(), 13, time.Duration(1)*time.Hour)

	first := <-batches
	second := <-batches

	assert.Len(t, first, 13)
	assert.Len(t, second, 13)

	assert.EqualValues(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"}, stringSlice(first))
	assert.EqualValues(t, []string{"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}, stringSlice(second))
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
