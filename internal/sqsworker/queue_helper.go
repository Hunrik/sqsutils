package sqsworker

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// QueueHelper interface
type QueueHelper interface {
	DeleteMessage(receiptHandle string)
	ChangeVisibility(receiptHandle string, seconds int64)
	ReceiveMessage() (*sqs.ReceiveMessageOutput, error)
}

type queue struct {
	queueURL  string
	Queue     *sqs.SQS
	rcvConfig *sqs.ReceiveMessageInput
}

func newQueueHelper(options Options) (*queue, error) {
	q := sqs.New(options.AWSConfig)

	rcvConfig := &sqs.ReceiveMessageInput{}
	rcvConfig.SetQueueUrl(options.QueueURL)
	rcvConfig.SetMaxNumberOfMessages(options.MaxNumberOfMessages)
	rcvConfig.SetWaitTimeSeconds(options.WaitTimeSeconds)
	rcvConfig.SetVisibilityTimeout(options.SQSVissiblityTimeout)

	if err := rcvConfig.Validate(); err != nil {
		return nil, err
	}
	return &queue{
		Queue:     q,
		queueURL:  options.QueueURL,
		rcvConfig: rcvConfig,
	}, nil
}

func (q *queue) DeleteMessage(receiptHandle string) {
	conf := &sqs.DeleteMessageInput{}
	conf.SetQueueUrl(q.queueURL)
	conf.SetReceiptHandle(receiptHandle)
	_, err := q.Queue.DeleteMessage(conf)
	if err != nil {
		fmt.Println(err)
	}
}

func (q *queue) ChangeVisibility(receiptHandle string, seconds int64) {
	conf := &sqs.ChangeMessageVisibilityInput{}
	conf.SetQueueUrl(q.queueURL)
	conf.SetReceiptHandle(receiptHandle)
	conf.SetVisibilityTimeout(seconds)
	_, err := q.Queue.ChangeMessageVisibility(conf)
	if err != nil {
		fmt.Println(err)
	}
}
func (q *queue) ReceiveMessage() (*sqs.ReceiveMessageOutput, error) {
	return q.Queue.ReceiveMessage(q.rcvConfig)
}
