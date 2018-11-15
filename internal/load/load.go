package load

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Hunrik/sqsutils/internal/sqs"
)

// Load .
func Load(filePath string, queueName string, formatString string) error {
	if formatString == "" {
		formatString = "%s"
	}
	flag.Parse()

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	q, err := sqs.NewSqs(queueName)
	if err != nil {
		return err
	}
	for i := 0; i < 64; i++ {
		go q.StartConsumer()
	}

	isFileDone := false
	go func() {
		reader := bufio.NewReaderSize(f, 262144)
		for {
			line, _, err := reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					isFileDone = true
					break
				}
				panic(err)
			}
			trimmed := fmt.Sprintf(formatString, line)
			q.Add(trimmed)
		}
	}()

	for isFileDone || q.InputCh.Len() == 0 {
		time.Sleep(500 * time.Millisecond)
	}
	q.Stop()
	return nil
}
