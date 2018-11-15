package save

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Hunrik/sqsutils/internal/sqsworker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/paulbellamy/ratecounter"
)

// Save .
func Save(queue, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	rate := ratecounter.NewRateCounter(5 * time.Second)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Rate ", rate.Rate()/10)
			}
		}
	}()

	h := &handler{w: w, rate: rate}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		return err
	}
	opts := sqsworker.Options{
		Handler:         h,
		MaxPrefechCount: 1000,
		MaxWorkerCount:  100,
		QueueURL:        queue,
		FetchThreads:    30,
		AWSConfig:       sess,
	}
	wrk, err := sqsworker.New(opts)
	if err != nil {
		return err
	}

	wrk.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()

	<-c
	fmt.Println("shutting down ....")
	wrk.Stop()
	w.Flush()
	time.Sleep(1 * time.Second)
	os.Exit(0)
	return nil
}

type handler struct {
	w    *bufio.Writer
	rate *ratecounter.RateCounter
	sync.Mutex
}

func (h *handler) Handle(message sqs.Message) bool {
	h.Lock()
	h.w.WriteString(*message.Body + "\n")
	h.rate.Incr(1)
	defer h.Unlock()
	return true
}
