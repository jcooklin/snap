/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package strategy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/serror"
)

type SubscriptionType int

const (
	// this subscription is bound to an explicit version
	BoundSubscriptionType SubscriptionType = iota
	// this subscription is akin to "latest" and must be moved if a newer version is loaded.
	UnboundSubscriptionType
)

var (
	// This defines the maximum running instances of a loaded plugin.
	// It is initialized at runtime via the cli.
	MaximumRunningPlugins = 3
)

var (
	ErrBadType     = errors.New("bad plugin type")
	ErrBadStrategy = errors.New("bad strategy")
	ErrPoolEmpty   = errors.New("plugin pool is empty")
)

type Pool interface {
	RoutingAndCaching
	Count() int
	Eligible() bool
	Insert(a AvailablePlugin) error
	Kill(id uint32, reason string)
	MoveSubscriptions(to Pool) []subscription
	Plugins() map[uint32]AvailablePlugin
	RLock()
	RUnlock()
	SelectAndKill(taskID, reason string)
	SelectAP(taskID string) (SelectablePlugin, serror.SnapError)
	Strategy() RoutingAndCaching
	Subscribe(taskID string, subType SubscriptionType)
	SubscriptionCount() int
	Unsubscribe(taskID string)
	Version() int
	RestartCount() int
	IncRestartCount()
}

type AvailablePlugin interface {
	core.AvailablePlugin
	CacheTTL() time.Duration
	CheckHealth()
	ConcurrencyCount() int
	Exclusive() bool
	Kill(r string) error
	RoutingStrategy() plugin.RoutingStrategyType
	SetID(id uint32)
	String() string
	Type() plugin.PluginType
}

type subscription struct {
	SubType SubscriptionType
	Version int
	TaskID  string
}

type pool struct {
	// used to coordinate changes to a pool
	*sync.RWMutex

	// the version of the plugins in the pool.
	// subscriptions uses this.
	version int
	// key is the primary key used in availablePlugins:
	// {plugin_type}:{plugin_name}:{plugin_version}
	key string

	// The subscriptions to this pool.
	subs map[string]*subscription

	// The plugins in the pool.
	// the primary key is an increasing --> uint from
	// snapd epoch (`service snapd start`).
	plugins    map[uint32]AvailablePlugin
	pidCounter uint32

	// The max size which this pool may grow.
	max int

	// The number of subscriptions per running instance
	concurrencyCount int

	// The routing and caching strategy declared by the plugin.
	// strategy RoutingAndCaching
	RoutingAndCaching

	// restartCount the restart count of available plugins
	// when the DeadAvailablePluginEvent occurs
	restartCount int
}

func NewPool(key string, plugins ...AvailablePlugin) (Pool, error) {
	versl := strings.Split(key, ":")
	ver, err := strconv.Atoi(versl[len(versl)-1])
	if err != nil {
		return nil, err
	}
	p := &pool{
		RWMutex:          &sync.RWMutex{},
		version:          ver,
		key:              key,
		subs:             make(map[string]*subscription),
		plugins:          make(map[uint32]AvailablePlugin),
		max:              MaximumRunningPlugins,
		concurrencyCount: 1,
	}

	if len(plugins) > 0 {
		for _, plg := range plugins {
			p.Insert(plg)
		}
	}

	return p, nil
}

// Version returns the version
func (p *pool) Version() int {
	return p.version
}

// Plugins returns a map of plugin ids to the AvailablePlugin
func (p *pool) Plugins() map[uint32]AvailablePlugin {
	return p.plugins
}

// Strategy returns the routing strategy
func (p *pool) Strategy() RoutingAndCaching {
	return p.RoutingAndCaching
}

// RestartCount returns the restart count of a pool
func (p *pool) RestartCount() int {
	return p.restartCount
}

func (p *pool) IncRestartCount() {
	p.RLock()
	defer p.RUnlock()
	p.restartCount++
}

// Insert inserts an AvailablePlugin into the pool
func (p *pool) Insert(a AvailablePlugin) error {
	if a.Type() != plugin.CollectorPluginType && a.Type() != plugin.ProcessorPluginType && a.Type() != plugin.PublisherPluginType {
		return ErrBadType
	}
	// If an empty pool is created, it does not have
	// any available plugins from which to retrieve
	// concurrency count or exclusivity.  We ensure it
	// is set correctly on an insert.
	if len(p.plugins) == 0 {
		if err := p.applyPluginMeta(a); err != nil {
			return err
		}
	}

	a.SetID(p.generatePID())
	p.plugins[a.ID()] = a

	return nil
}

// applyPluginMeta is called when the first plugin is added to the pool
func (p *pool) applyPluginMeta(a AvailablePlugin) error {
	// Checking if plugin is exclusive
	// (only one instance should be running).
	if a.Exclusive() {
		p.max = 1
	}

	// Set the cache TTL
	cacheTTL := GlobalCacheExpiration
	// if the plugin exposes a default TTL that is greater the the global default use it
	if a.CacheTTL() != 0 && a.CacheTTL() > GlobalCacheExpiration {
		cacheTTL = a.CacheTTL()
	}

	// Set the concurrency count
	p.concurrencyCount = a.ConcurrencyCount()

	// Set the routing and caching strategy
	switch a.RoutingStrategy() {
	case plugin.DefaultRouting:
		p.RoutingAndCaching = NewLRU(cacheTTL)
	case plugin.StickyRouting:
		p.RoutingAndCaching = NewSticky(cacheTTL)
		p.concurrencyCount = 1
	default:
		return ErrBadStrategy
	}

	return nil
}

// subscribe adds a subscription to the pool.
// Using subscribe is idempotent.
func (p *pool) Subscribe(taskID string, subType SubscriptionType) {
	p.Lock()
	defer p.Unlock()

	if _, exists := p.subs[taskID]; !exists {
		// Version is the last item in the key, so we split here
		// to retrieve it for the subscription.
		p.subs[taskID] = &subscription{
			TaskID:  taskID,
			SubType: subType,
			Version: p.version,
		}
	}
	fmt.Println("\n\n In subscribe subtype: ", subType)
	fmt.Println()
}

// unsubscribe removes a subscription from the pool.
// Using unsubscribe is idempotent.
func (p *pool) Unsubscribe(taskID string) {
	p.Lock()
	defer p.Unlock()
	delete(p.subs, taskID)
}

// Eligible returns a bool indicating whether the pool is eligible to grow
func (p *pool) Eligible() bool {
	p.RLock()
	defer p.RUnlock()

	// optimization: don't even bother with concurrency
	// count if we have already reached pool max
	if p.Count() == p.max {
		return false
	}

	should := p.SubscriptionCount() / p.concurrencyCount
	if should > p.Count() && should <= p.max {
		return true
	}

	return false
}

// kill kills and removes the available plugin from its pool.
// Using kill is idempotent.
func (p *pool) Kill(id uint32, reason string) {
	p.Lock()
	defer p.Unlock()

	ap, ok := p.plugins[id]
	if ok {
		ap.Kill(reason)
		delete(p.plugins, id)
	}
}

// SelectAndKill selects, kills and removes the available plugin from the pool
func (p *pool) SelectAndKill(taskID, reason string) {
	sp := make([]SelectablePlugin, p.Count())
	i := 0
	for _, plg := range p.plugins {
		sp[i] = plg
		i++
	}
	rp, err := p.Remove(sp, taskID)
	if err != nil {
		log.WithFields(log.Fields{
			"_block": "selectAndKill",
			"taskID": taskID,
			"reason": reason,
		}).Error(err)
	}
	if err := rp.Kill(reason); err != nil {
		log.WithFields(log.Fields{
			"_block": "selectAndKill",
			"taskID": taskID,
			"reason": reason,
		}).Error(err)
	}
	p.remove(rp.ID())
}

// remove removes an available plugin from the the pool.
// using remove is idempotent.
func (p *pool) remove(id uint32) {
	p.Lock()
	defer p.Unlock()
	delete(p.plugins, id)
}

// Count returns the number of plugins in the pool
func (p *pool) Count() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.plugins)
}

// NOTE: The data returned by subscriptions should be constant and read only.
func (p *pool) subscriptions() map[string]*subscription {
	p.RLock()
	defer p.RUnlock()
	return p.subs
}

// SubscriptionCount returns the number of subscriptions in the pool
func (p *pool) SubscriptionCount() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.subs)
}

// SelectAP selects an available plugin from the pool
func (p *pool) SelectAP(taskID string) (SelectablePlugin, serror.SnapError) {
	p.RLock()
	defer p.RUnlock()

	sp := make([]SelectablePlugin, p.Count())
	i := 0
	for _, plg := range p.plugins {
		sp[i] = plg
		i++
	}
	sap, err := p.Select(sp, taskID)
	if err != nil || sap == nil {
		return nil, serror.New(err)
	}
	return sap, nil
}

// generatePID returns the next available pid for the pool
func (p *pool) generatePID() uint32 {
	atomic.AddUint32(&p.pidCounter, 1)
	return p.pidCounter
}

// MoveSubscriptions moves subscriptions to another pool
func (p *pool) MoveSubscriptions(to Pool) []subscription {
	var subs []subscription

	if to.(*pool) == p {
		panic("omg")
	}
	p.Lock()
	defer p.Unlock()
	fmt.Println("\n\nAll My subs: ", p.subs)
	fmt.Println()
	fmt.Println("\n\b to subs", to.Count())
	fmt.Println()
	for task, sub := range p.subs {
		fmt.Println("\n\nType:", sub.SubType, "\nVersion:", sub.Version, "\nTaskID:", sub.TaskID)
		fmt.Println()
		// ensure that this sub was not bound to this pool specifically before moving
		if sub.SubType == UnboundSubscriptionType {
			subs = append(subs, *sub)
			to.Subscribe(task, UnboundSubscriptionType)
			delete(p.subs, task)
		}
	}
	fmt.Println("\n\b to subs after", to.Count())
	fmt.Println()
	return subs
}

// CacheTTL returns the cacheTTL for the pool
func (p *pool) CacheTTL(taskID string) (time.Duration, error) {
	if len(p.plugins) == 0 {
		return 0, ErrPoolEmpty
	}
	return p.RoutingAndCaching.CacheTTL(taskID)
}
