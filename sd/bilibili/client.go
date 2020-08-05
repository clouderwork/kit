package bilibili

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/bilibili/discovery/naming"
)

var (
	// ErrNoKey indicates a client method needs a key but receives none.
	ErrNoKey = errors.New("no key provided")

	// ErrNoValue indicates a client method needs a value but receives none.
	ErrNoValue = errors.New("no value provided")
)

// Client is a wrapper around the etcd client.
type Client interface {
	// GetEntries queries the given prefix in etcd and returns a slice
	// containing the values of all keys found, recursively, underneath that
	// prefix.
	GetEntries() ([]string, error)

	// WatchPrefix watches the given prefix in etcd for changes. When a change
	// is detected, it will signal on the passed channel. Clients are expected
	// to call GetEntries to update themselves with the latest set of complete
	// values. WatchPrefix will always send an initial sentinel value on the
	// channel after establishing the watch, to ensure that clients always
	// receive the latest set of values. WatchPrefix will block until the
	// context passed to the NewClient constructor is terminated.
	WatchPrefix(prefix string, ch chan struct{})

	// Register a service with etcd.
	Register(s Service) error

	// Deregister a service with etcd.
	Deregister(s Service) error
}

type client struct {
	cli    *naming.Discovery
	config *naming.Config
	ctx    context.Context

	// Watcher interface instance, used to leverage Watcher.Close()
	watcher naming.Resolver
	// watcher cancel func
	wcf context.CancelFunc

	weight int
}

// ClientOptions defines options for the etcd client. All values are optional.
// If any duration is not specified, a default of 3 seconds will be used.
type ClientOptions struct {
	Cert          string
	Key           string
	CACert        string
	DialTimeout   time.Duration
	DialKeepAlive time.Duration
	Username      string
	Password      string
}

// NewClient returns Client with a connection to the named machines. It will
// return an error if a connection to the cluster cannot be made.
func NewClient(ctx context.Context, config *naming.Config, weight int) (Client, error) {
	dis := naming.New(config)

	return &client{
		cli:    dis,
		config: config,
		ctx:    ctx,
		weight: weight,
	}, nil
}

// GetEntries implements the etcd Client interface.
func (c *client) GetEntries() ([]string, error) {
	var entries []string
	mods, ok := c.watcher.Fetch()
	if ok {
		ins := mods.Instances[c.config.Zone]
		entries = make([]string, 0, len(ins))

		for _, in := range ins {
			for _, addrs := range in.Addrs {
				u, err := url.Parse(addrs)
				if err == nil && u.Scheme == "grpc" {
					entries = append(entries, u.Host)
				}
			}
		}
	}

	return entries, nil
}

// WatchPrefix implements the etcd Client interface.
func (c *client) WatchPrefix(prefix string, ch chan struct{}) {
	c.watcher = c.cli.Build(prefix)
	wch := c.watcher.Watch()
	ch <- struct{}{}
	for _ = range wch {
		ch <- struct{}{}
	}
}

func (c *client) Register(s Service) error {
	appID := s.Key
	region := c.config.Region
	zone := c.config.Zone
	env := c.config.Env
	host := c.config.Host

	addr := s.Value
	ins := &naming.Instance{
		Region:   region,
		Zone:     zone,
		Env:      env,
		Hostname: host,
		AppID:    appID,
		Addrs: []string{
			// "grpc://" + addr + ":" + port,
			addr,
		},
		Metadata: map[string]string{
			naming.MetaWeight: strconv.Itoa(c.weight),
		},
	}

	var err error
	c.wcf, err = c.cli.Register(ins)
	return err
}

func (c *client) Deregister(s Service) error {
	c.wcf()
	return nil
}
