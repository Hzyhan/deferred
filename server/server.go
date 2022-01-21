package server

import (
	"context"
	brokersiface "deferred/broker"
	"deferred/config"
	"deferred/tasks"
	"deferred/worker"
	"fmt"
	"log"
	"sync"

	"github.com/robfig/cron/v3"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *config.Config
	registeredTasks   *sync.Map
	broker            brokersiface.Broker
	scheduler         *cron.Cron
	prePublishHandler func(*tasks.Signature)
}

// NewServer creates Server instance
func NewServer(cnf *config.Config, brokerServer brokersiface.Broker) *Server {
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map),
		broker:          brokerServer,
		scheduler:       cron.New(),
	}

	// Run scheduler job
	go srv.scheduler.Run()

	return srv
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *worker.Worker {
	return &worker.Worker{
		Server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *worker.Worker {
	return &worker.Worker{
		Server:      server,
		ConsumerTag: consumerTag,
		Concurrency: concurrency,
		Queue:       queue,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

// GetConfig returns connection object
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// SetConfig sets config
func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	for _, task := range namedTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	for k, v := range namedTaskFuncs {
		server.registeredTasks.Store(k, v)
	}
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks.Store(name, taskFunc)
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks.Load(name)
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) {

}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *tasks.Signature) {

}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) {

}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *tasks.Chain) {

}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) {

}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) {

}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) {

}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *tasks.Chord, sendConcurrency int) {

}

// GetRegisteredTaskNames returns slice of registered task names
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, 0)

	server.registeredTasks.Range(func(key, value interface{}) bool {
		taskNames = append(taskNames, key.(string))
		return true
	})
	return taskNames
}

// RegisterPeriodicTask register a periodic task which will be triggered periodically
func (server *Server) RegisterPeriodicTask(spec, name string, signature *tasks.Signature) error {
	//check spec
	_, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		//get lock

		//send task
		server.SendTask(tasks.CopySignature(signature))
		if err != nil {
			log.Printf("ERROR.periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChain register a periodic chain which will be triggered periodically
func (server *Server) RegisterPeriodicChain(spec, name string, signatures ...*tasks.Signature) error {
	//check spec
	_, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chain
		chain, _ := tasks.NewChain(tasks.CopySignatures(signatures...)...)

		//get lock

		//send task
		server.SendChain(chain)
		if err != nil {
			log.Printf("ERROR.periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}
