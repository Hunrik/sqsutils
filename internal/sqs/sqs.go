package sqs

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/paulbellamy/ratecounter"
	"gopkg.in/cheggaaa/mb.v1"
)

type Queue struct {
	wg       sync.WaitGroup
	queue    *sqs.SQS
	InputCh  *mb.MB
	ctx      context.Context
	cancel   context.CancelFunc
	queueURL string
	counter  *ratecounter.RateCounter
}

func NewSqs(queueURL string) (*Queue, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	q := sqs.New(sess)
	ctx, cancel := context.WithCancel(context.Background())
	counter := ratecounter.NewRateCounter(5 * time.Second)

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Rate ", counter.Rate()/5)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	return &Queue{
		queue:    q,
		InputCh:  mb.New(1000000),
		ctx:      ctx,
		cancel:   cancel,
		queueURL: queueURL,
		counter:  counter,
	}, nil
}

func (q *Queue) StartConsumer() {
	go func() {
		q.wg.Add(1)
		for {
			select {
			case <-q.ctx.Done():
				q.wg.Done()
				return
			default:
				sqsMessage := sqs.SendMessageBatchInput{
					QueueUrl: &q.queueURL,
				}
				messages := q.InputCh.WaitMax(10)
				if len(messages) == 0 {
					continue
				}
				for _, message := range messages {
					val, ok := message.(string)
					if !ok {
						panic("lol")
					}
					id := uuid.New().String()
					entry := sqs.SendMessageBatchRequestEntry{
						MessageBody: &val,
						Id:          &id,
					}
					sqsMessage.Entries = append(sqsMessage.Entries, &entry)
				}
				r, err := q.queue.SendMessageBatch(&sqsMessage)
				if err != nil {
					log.Println(err)
				}

				q.counter.Incr(int64(len(r.Successful)))
			}
		}
	}()
}
func (q *Queue) Stop() {
	q.cancel()
	q.InputCh.Close()
	q.wg.Wait()
}
func (q *Queue) Add(doc interface{}) {
	q.InputCh.Add(doc)
}
