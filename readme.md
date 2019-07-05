# bach [![Build Status](https://travis-ci.org/bendrucker/bach.svg?branch=master)](https://travis-ci.org/bendrucker/bach) [![GoDoc](https://godoc.org/github.com/bendrucker/bach?status.svg)](https://godoc.org/github.com/bendrucker/bach)

Bach provides batching for channels in Go, flushing batches of messages based on a time interval and a maximum buffer size.

## Usage

```go
import "github.com/bendrucker/bach"

func main() {
	numbers := make(chan interface{}, 10)
	batches := bach.NewBatch(numbers, 10, time.Duration(100))

	numbers <- 1
	numbers <- 2
	numbers <- 3
	close(numbers)

	for batch := range batches {
		fmt.Println(batch)
	}

  // Output: [1, 2, 3]
}
```
