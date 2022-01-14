package cosnsume

import (
	"bytes"
	"context"
	"deferred/common"
	"deferred/config"
	"deferred/errs"
	"deferred/iface"
	"deferred/queue"
	"deferred/tasks"
	"encoding/json"
	"log"
	"runtime"
	"sync"
	"time"
)

// BrokerGR represents a Redis broker
type BrokerGR struct {
	common.Broker
	queue                queue.IQueue
	consumingWG          sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG         sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG            sync.WaitGroup
	redisDelayedTasksKey string
}

// StartConsuming enters a loop and waits for incoming messages
func (b *BrokerGR) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.Print("[*] INFO.Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				task, _ := b.nextTask(getQueueGR(b.GetConfig(), taskProcessor))
				//TODO: should this error be ignored?
				if len(task) > 0 {
					deliveries <- task
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(b.redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.Printf("Error.Decode err:%v", err)
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.Printf("ERROR.Publish err:%v", err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *BrokerGR) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	b.queue.Close()
}

// Publish places a new message on the default queue
func (b *BrokerGR) Publish(ctx context.Context, signature *tasks.Signature) error {

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			return b.queue.Enqueue(signature)
		}
	}

	return b.queue.Enqueue(signature)
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *BrokerGR) GetPendingTasks(queue string) ([]*tasks.Signature, error) {

	taskSignatures := b.queue.GetAll()
	return taskSignatures, nil
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *BrokerGR) GetDelayedTasks() ([]*tasks.Signature, error) {
	taskSignatures := b.queue.GetAll()
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *BrokerGR) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *BrokerGR) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(delivery, err)
	}

	log.Printf("DEBUG.Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *BrokerGR) nextTask(queue string) (result []byte, err error) {
	signature := b.queue.Dequeue()
	return json.Marshal(signature)
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func (b *BrokerGR) nextDelayedTask(key string) (result []byte, err error) {

	pollPeriod := 500 // default poll period for delayed tasks

	for {
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		signature := b.queue.Peek()
		if signature == nil {
			continue
		}
		if signature.ETA != nil {
			now := time.Now().UTC()

			if signature.ETA.After(now) {
				sign := b.queue.Dequeue()
				return json.Marshal(sign)
			}
		}
	}
}

func getQueueGR(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
