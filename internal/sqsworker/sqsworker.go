package sqsworker

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jpillora/backoff"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type message struct {
	deliveredAt time.Time
	message     sqs.Message
}

// Handler  .
type Handler interface {
	Handle(sqs.Message) bool
}

type messageQueue chan message

type logger interface {
	Error(msg string, fields ...zapcore.Field)
	Warn(msg string, fields ...zapcore.Field)
}

// Worker .
type Worker struct {
	handler   Handler
	messageCh chan message

	SQSVissiblityTimeout int64
	maxWorkers           int
	fetchThreads         int
	logger               logger
	queue                QueueHelper
	ctx                  context.Context
	cancel               context.CancelFunc
	backoff              *backoff.Backoff
	sync.Mutex
}

// Options .
type Options struct {
	Handler              Handler
	MaxPrefechCount      int
	MaxWorkerCount       int
	AWSConfig            client.ConfigProvider
	QueueURL             string
	FetchThreads         int
	MaxNumberOfMessages  int64
	WaitTimeSeconds      int64
	Logger               logger
	SQSVissiblityTimeout int64
}

// New worker
func New(options Options) (*Worker, error) {
	if options.MaxPrefechCount == 0 {
		options.MaxPrefechCount = 100
	}
	if options.MaxWorkerCount == 0 {
		options.MaxWorkerCount = 50
	}
	if options.FetchThreads == 0 {
		options.FetchThreads = 2
	}
	if options.WaitTimeSeconds == 0 {
		options.WaitTimeSeconds = 20
	}
	if options.MaxNumberOfMessages == 0 {
		options.MaxNumberOfMessages = 10
	}
	if options.SQSVissiblityTimeout == 0 {
		options.SQSVissiblityTimeout = 20
	}
	if options.Logger == nil {
		logger, _ := zap.NewProduction()
		options.Logger = logger
	}
	q, err := newQueueHelper(options)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    30 * time.Second,
		Factor: 2,
		Jitter: true,
	}

	w := &Worker{
		handler:              options.Handler,
		messageCh:            make(chan message, options.MaxPrefechCount),
		maxWorkers:           options.MaxWorkerCount,
		fetchThreads:         options.FetchThreads,
		ctx:                  ctx,
		cancel:               cancel,
		queue:                q,
		logger:               options.Logger,
		backoff:              b,
		SQSVissiblityTimeout: options.SQSVissiblityTimeout,
	}

	return w, nil
}
func (w *Worker) spawnWorker() {
	for {
		select {
		case job := <-w.messageCh:
			age := time.Duration.Seconds(time.Since(job.deliveredAt))
			if int64(age) >= w.SQSVissiblityTimeout {
				continue
			}

			if age > 10 {
				w.queue.ChangeVisibility(*job.message.ReceiptHandle, w.SQSVissiblityTimeout)
			}
			canDelete := w.handler.Handle(job.message)
			if canDelete {
				w.queue.DeleteMessage(*job.message.ReceiptHandle)
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) spawnFetchThread() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			messages, err := w.queue.ReceiveMessage()
			if err != nil {
				w.Lock()
				sleep := w.backoff.Duration()
				w.Unlock()
				time.Sleep(sleep)
				if w.logger != nil {
					w.logger.Error("SQS Fetch error", zap.Error(err))
				}
				continue
			}
			w.Lock()
			w.backoff.Reset()
			w.Unlock()
			for _, sqsMessage := range messages.Messages {
				message := message{
					message:     *sqsMessage,
					deliveredAt: time.Now(),
				}
				w.messageCh <- message
			}
		}
	}
}

func (w *Worker) startFetch() error {
	for i := 0; i < w.fetchThreads; i++ {
		go w.spawnFetchThread()
	}
	return nil
}

// Start the worker
func (w *Worker) Start() error {
	for i := 0; i < w.maxWorkers; i++ {
		go w.spawnWorker()
	}
	err := w.startFetch()
	if err != nil {
		w.Stop()
	}
	return err
}

// Stop stops the worker
func (w *Worker) Stop() {
	w.cancel()
	// Drain queue
	for len(w.messageCh) > 0 {
		<-w.messageCh
	}
}
