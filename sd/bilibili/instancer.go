package bilibili

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/sd"
	"github.com/go-kit/kit/sd/internal/instance"
)

// Instancer yields instances stored in a certain etcd keyspace. Any kind of
// change in that keyspace is watched and will update the Instancer's Instancers.
type Instancer struct {
	cache  *instance.Cache
	client Client
	appid  string
	logger log.Logger
	quitc  chan struct{}
}

// NewInstancer returns an etcd instancer. It will start watching the given
// appid for changes, and update the subscribers.
func NewInstancer(c Client, appid string, logger log.Logger) (*Instancer, error) {
	s := &Instancer{
		client: c,
		appid:  appid,
		cache:  instance.NewCache(),
		logger: logger,
		quitc:  make(chan struct{}),
	}
	go s.loop()
	return s, nil
}

func (s *Instancer) loop() {
	ch := make(chan struct{})
	go s.client.WatchPrefix(s.appid, ch)

	for {
		select {
		case <-ch:
			instances, err := s.client.GetEntries()
			if err != nil {
				s.logger.Log("msg", "failed to retrieve entries", "err", err)
				s.cache.Update(sd.Event{Err: err})
				continue
			}
			s.cache.Update(sd.Event{Instances: instances})

		case <-s.quitc:
			return
		}
	}
}

// Stop terminates the Instancer.
func (s *Instancer) Stop() {
	close(s.quitc)
}

// Register implements Instancer.
func (s *Instancer) Register(ch chan<- sd.Event) {
	s.cache.Register(ch)
}

// Deregister implements Instancer.
func (s *Instancer) Deregister(ch chan<- sd.Event) {
	s.cache.Deregister(ch)
}
