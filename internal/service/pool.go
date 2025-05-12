package service

import (
	"context"
	"etl-pipeline/config"
	"sync"
)

type Task func(ctx context.Context)

type Pool interface {
	Start()
	Submit(task Task)
	Stop()
}

type pool struct {
	numberWorker int
	tasks        chan Task
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewPool(config *config.Config) Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &pool{
		numberWorker: config.Worker.NumWorkers,
		tasks:        make(chan Task, 1000),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (p *pool) Start() {
	for i := 0; i < p.numberWorker; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case task := <-p.tasks:
					task(p.ctx)
				}
			}
		}(i)
	}
}

func (p *pool) Submit(task Task) {
	p.tasks <- task
}

func (p *pool) Stop() {
	p.cancel()
	p.wg.Wait()
}
