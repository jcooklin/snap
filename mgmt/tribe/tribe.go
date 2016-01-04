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

package tribe

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/memberlist"
	"github.com/intelsdi-x/gomit"
	"github.com/pborman/uuid"

	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/control_event"
	"github.com/intelsdi-x/snap/core/scheduler_event"
	"github.com/intelsdi-x/snap/core/serror"
	"github.com/intelsdi-x/snap/mgmt/tribe/agreement"
	"github.com/intelsdi-x/snap/mgmt/tribe/worker"
)

const (
	HandlerRegistrationName = "tribe"
)

var (
	errAgreementDoesNotExist          = errors.New("Agreement does not exist")
	errAgreementAlreadyExists         = errors.New("Agreement already exists")
	errUnknownMember                  = errors.New("Unknown member")
	errAlreadyMemberOfPluginAgreement = errors.New("Already a member of a plugin agreement")
	errNotAMember                     = errors.New("Not a member of agreement")
	errTaskAlreadyExists              = errors.New("Task already exists")
	errTaskDoesNotExist               = errors.New("Task does not exist")
	errCreateMemberlist               = errors.New("Failed to start tribe")
	errMemberlistJoin                 = errors.New("Failed to join tribe")
	errPluginCatalogNotSet            = errors.New("Plugin Catalog not set")
	errTaskManagerNotSet              = errors.New("Task Manager not set")
)

var logger = log.WithFields(log.Fields{
	"_module": "tribe",
})

type tribe struct {
	clock              LClock
	agreements         map[string]*agreement.Agreement
	mutex              sync.RWMutex
	msgBuffer          []msg
	intentBuffer       []msg
	broadcasts         *memberlist.TransmitLimitedQueue
	memberlist         *memberlist.Memberlist
	logger             *log.Entry
	taskStartStopCache *cache
	taskStateResponses map[string]*taskStateQueryResponse
	members            map[string]*agreement.Member
	tags               map[string]string
	config             *config
	counters           *counters

	pluginCatalog   worker.ManagesPlugins
	taskManager     worker.ManagesTasks
	pluginWorkQueue chan worker.PluginRequest
	taskWorkQueue   chan worker.TaskRequest

	workerQuitChan  chan struct{}
	workerWaitGroup *sync.WaitGroup
}

type config struct {
	seed                      string
	restAPIPort               int
	restAPIProto              string
	restAPIInsecureSkipVerify string
	MemberlistConfig          *memberlist.Config
}

type Opt func(*tribe)

func EnableCounters() Opt {
	return func(t *tribe) {
		t.counters = newCounters()
	}
}

func DisableCounters() Opt {
	return func(t *tribe) {
		t.counters = nil
	}
}

func DefaultConfig(name, advertiseAddr string, advertisePort int, seed string, restAPIPort int) *config {
	c := &config{
		seed:         seed,
		restAPIPort:  restAPIPort,
		restAPIProto: "http",
	}
	c.MemberlistConfig = memberlist.DefaultLANConfig()
	c.MemberlistConfig.PushPullInterval = 300 * time.Second
	c.MemberlistConfig.Name = name
	c.MemberlistConfig.BindAddr = advertiseAddr
	c.MemberlistConfig.BindPort = advertisePort
	c.MemberlistConfig.GossipNodes = c.MemberlistConfig.GossipNodes * 2

	return c
}

func New(c *config, opts ...Opt) (*tribe, error) {
	logger := logger.WithFields(log.Fields{
		"port": c.MemberlistConfig.BindPort,
		"addr": c.MemberlistConfig.BindAddr,
		"name": c.MemberlistConfig.Name,
	})

	tribe := &tribe{
		agreements:         map[string]*agreement.Agreement{},
		members:            map[string]*agreement.Member{},
		taskStateResponses: map[string]*taskStateQueryResponse{},
		taskStartStopCache: newCache(),
		msgBuffer:          make([]msg, 512),
		intentBuffer:       []msg{},
		logger:             logger.WithField("_name", c.MemberlistConfig.Name),
		tags: map[string]string{
			agreement.RestPort:               strconv.Itoa(c.restAPIPort),
			agreement.RestProtocol:           c.restAPIProto,
			agreement.RestInsecureSkipVerify: c.restAPIInsecureSkipVerify,
		},
		pluginWorkQueue: make(chan worker.PluginRequest, 999),
		taskWorkQueue:   make(chan worker.TaskRequest, 999),
		workerQuitChan:  make(chan struct{}),
		workerWaitGroup: &sync.WaitGroup{},
		config:          c,
	}

	tribe.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return len(tribe.memberlist.Members())
		},
		RetransmitMult: memberlist.DefaultLANConfig().RetransmitMult,
	}

	//configure delegates
	c.MemberlistConfig.Delegate = newDelegate(tribe)
	c.MemberlistConfig.Events = &memberDelegate{tribe: tribe}

	ml, err := memberlist.Create(c.MemberlistConfig)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	tribe.memberlist = ml

	for _, opt := range opts {
		opt(tribe)
	}

	if c.seed != "" {
		_, err := ml.Join([]string{c.seed})
		if err != nil {
			logger.WithFields(log.Fields{
				"seed": c.seed,
			}).Error(errMemberlistJoin)
			return nil, errMemberlistJoin
		}
		logger.WithFields(log.Fields{
			"seed": c.seed,
		}).Infoln("tribe started")
		return tribe, nil
	}
	logger.WithFields(log.Fields{
		"seed": "none",
	}).Infoln("tribe started")
	return tribe, nil
}

type cache struct {
	sync.RWMutex
	table map[string]time.Time
}

func newCache() *cache {
	return &cache{
		table: make(map[string]time.Time),
	}
}

func (c *cache) get(m msg) (time.Time, bool) {
	c.RLock()
	defer c.RUnlock()
	key := fmt.Sprintf("%v:%v", m.GetType(), m.ID())
	v, ok := c.table[key]
	return v, ok
}

func (c *cache) put(m msg, duration time.Duration) bool {
	c.Lock()
	defer c.Unlock()
	key := fmt.Sprintf("%v:%v", m.GetType(), m.ID())
	if ti, ok := c.table[key]; ok {
		logger.WithFields(log.Fields{
			"task-id":      m.ID(),
			"time-started": ti,
			"_block":       "task-cache-put",
			"key":          key,
			"cache-size":   len(c.table),
			"ltime":        m.Time(),
		}).Debugln("task cache entry exists")
		return false
	}
	c.table[key] = time.Now()
	time.AfterFunc(duration, func() {
		c.Lock()
		delete(c.table, key)
		c.Unlock()
	})
	return true
}

func (t *tribe) SetPluginCatalog(p worker.ManagesPlugins) {
	t.pluginCatalog = p
}

func (t *tribe) SetTaskManager(m worker.ManagesTasks) {
	t.taskManager = m
}

func (t *tribe) Name() string {
	return "tribe"
}

func (t *tribe) Start() error {
	if t.pluginCatalog == nil {
		return errPluginCatalogNotSet
	}
	if t.taskManager == nil {
		return errTaskManagerNotSet
	}
	worker.DispatchWorkers(
		4,
		t.pluginWorkQueue,
		t.taskWorkQueue,
		t.workerQuitChan,
		t.workerWaitGroup,
		t.pluginCatalog,
		t.taskManager,
		t)
	return nil
}

func (t *tribe) Stop() {
	logger := t.logger.WithFields(log.Fields{
		"_block": "stop",
	})
	err := t.memberlist.Leave(1 * time.Second)
	if err != nil {
		logger.Error(err)
	}
	err = t.memberlist.Shutdown()
	if err != nil {
		logger.Error(err)
	}
	close(t.workerQuitChan)
	t.workerWaitGroup.Wait()
}

func (t *tribe) GetTaskAgreementMembers() ([]worker.Member, error) {
	m, ok := t.members[t.memberlist.LocalNode().Name]
	if !ok || m.TaskAgreements == nil {
		return nil, errNotAMember
	}

	mm := map[*agreement.Member]struct{}{}
	for name := range m.TaskAgreements {
		for _, mem := range t.agreements[name].Members {
			mm[mem] = struct{}{}
		}
	}
	members := make([]worker.Member, 0, len(mm))
	for k := range mm {
		members = append(members, k)
	}
	return members, nil
}

func (t *tribe) GetPluginAgreementMembers() ([]worker.Member, error) {
	m, ok := t.members[t.memberlist.LocalNode().Name]
	if !ok || m.PluginAgreement == nil {
		return nil, errNotAMember
	}
	members := make([]worker.Member, 0, len(t.agreements[m.PluginAgreement.Name].Members))
	for _, v := range t.agreements[m.PluginAgreement.Name].Members {
		members = append(members, v)
	}
	return members, nil
}

// HandleGomitEvent handles events emitted from control
func (t *tribe) HandleGomitEvent(e gomit.Event) {
	logger := t.logger.WithFields(log.Fields{
		"_block": "handle-gomit-event",
	})
	switch v := e.Body.(type) {
	case *control_event.LoadPluginEvent:
		logger.WithFields(log.Fields{
			"event":          e.Namespace(),
			"plugin-name":    v.Name,
			"plugin-version": v.Version,
			"plugin-type":    core.PluginType(v.Type).String(),
		}).Debugf("handling load plugin event")
		plugin := agreement.Plugin{
			Name_:    v.Name,
			Version_: v.Version,
			Type_:    core.PluginType(v.Type),
		}
		if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
			if m.PluginAgreement != nil {
				if ok, _ := m.PluginAgreement.Plugins.Contains(plugin); !ok {
					t.AddPlugin(m.PluginAgreement.Name, plugin)
				}
			}
		}
	case *control_event.UnloadPluginEvent:
		logger.WithFields(log.Fields{
			"event":          e.Namespace(),
			"plugin-name":    v.Name,
			"plugin-version": v.Version,
			"plugin-type":    core.PluginType(v.Type).String(),
		}).Debugf("handling unload plugin event")
		plugin := agreement.Plugin{
			Name_:    v.Name,
			Version_: v.Version,
			Type_:    core.PluginType(v.Type),
		}
		if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
			if m.PluginAgreement != nil {
				if ok, _ := m.PluginAgreement.Plugins.Contains(plugin); ok {
					t.RemovePlugin(m.PluginAgreement.Name, plugin)
				}
			}
		}
	case *scheduler_event.TaskCreatedEvent:
		if v.Source != "tribe" {
			logger.WithFields(log.Fields{
				"event":                e.Namespace(),
				"task-id":              v.TaskID,
				"task-start-on-create": v.StartOnCreate,
			}).Debugf("handling task create event")
			task := agreement.Task{
				ID:            v.TaskID,
				StartOnCreate: v.StartOnCreate,
			}
			if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
				if m.TaskAgreements != nil {
					for n, a := range m.TaskAgreements {
						if ok, _ := a.Tasks.Contains(task); !ok {
							t.AddTask(n, task)
						}
					}
				}
			}
		}
	case *scheduler_event.TaskStoppedEvent:
		if v.Source != "tribe" {
			logger.WithFields(log.Fields{
				"event":   e.Namespace(),
				"task-id": v.TaskID,
			}).Debugf("handling task stop event")
			task := agreement.Task{
				ID: v.TaskID,
			}
			if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
				if m.TaskAgreements != nil {
					for n, a := range m.TaskAgreements {
						if ok, _ := a.Tasks.Contains(task); ok {
							t.StopTask(n, task)
						}
					}
				}
			}
		}
	case *scheduler_event.TaskStartedEvent:
		if v.Source != "tribe" {
			logger.WithFields(log.Fields{
				"event":   e.Namespace(),
				"task-id": v.TaskID,
			}).Debugf("handling task start event")
			task := agreement.Task{
				ID: v.TaskID,
			}
			if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
				if m.TaskAgreements != nil {
					for n, a := range m.TaskAgreements {
						if ok, _ := a.Tasks.Contains(task); ok {
							t.StartTask(n, task)
						}
					}
				}
			}
		}
	case *scheduler_event.TaskDeletedEvent:
		if v.Source != "tribe" {
			logger.WithFields(log.Fields{
				"event":   e.Namespace(),
				"task-id": v.TaskID,
			}).Debugf("handling task start event")
			task := agreement.Task{
				ID: v.TaskID,
			}
			if m, ok := t.members[t.memberlist.LocalNode().Name]; ok {
				if m.TaskAgreements != nil {
					for n, a := range m.TaskAgreements {
						if ok, _ := a.Tasks.Contains(task); ok {
							t.RemoveTask(n, task)
						}
					}
				}
			}
		}
	}
}

func (t *tribe) GetMember(name string) *agreement.Member {
	if m, ok := t.members[name]; ok {
		return m
	}
	return nil
}

func (t *tribe) GetMembers() []string {
	var members []string
	for _, member := range t.memberlist.Members() {
		members = append(members, member.Name)
	}
	return members
}

func (t *tribe) LeaveAgreement(agreementName, memberName string) serror.SnapError {
	if err := t.canLeaveAgreement(agreementName, memberName); err != nil {
		return err
	}

	msg := &agreementMsg{
		LTime:         t.clock.Increment(),
		UUID:          uuid.New(),
		AgreementName: agreementName,
		MemberName:    memberName,
		Type:          leaveAgreementMsgType,
	}
	if t.handleLeaveAgreement(msg) {
		t.broadcast(leaveAgreementMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) JoinAgreement(agreementName, memberName string) serror.SnapError {
	if err := t.canJoinAgreement(agreementName, memberName); err != nil {
		return err
	}

	msg := &agreementMsg{
		LTime:         t.clock.Increment(),
		UUID:          uuid.New(),
		AgreementName: agreementName,
		MemberName:    memberName,
		Type:          joinAgreementMsgType,
	}
	if t.handleJoinAgreement(msg) {
		t.broadcast(joinAgreementMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) AddPlugin(agreementName string, p agreement.Plugin) error {
	if _, ok := t.agreements[agreementName]; !ok {
		return errAgreementDoesNotExist
	}
	msg := &pluginMsg{
		LTime:         t.clock.Increment(),
		Plugin:        p,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          addPluginMsgType,
	}
	if t.handleAddPlugin(msg) {
		t.broadcast(addPluginMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) RemovePlugin(agreementName string, p agreement.Plugin) error {
	if _, ok := t.agreements[agreementName]; !ok {
		return errAgreementDoesNotExist
	}
	msg := &pluginMsg{
		LTime:         t.clock.Increment(),
		Plugin:        p,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          removePluginMsgType,
	}
	if t.handleRemovePlugin(msg) {
		t.broadcast(removePluginMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) GetAgreement(name string) (*agreement.Agreement, serror.SnapError) {
	a, ok := t.agreements[name]
	if !ok {
		return nil, serror.New(errAgreementDoesNotExist, map[string]interface{}{"agreement_name": name})
	}
	return a, nil
}

func (t *tribe) GetAgreements() map[string]*agreement.Agreement {
	return t.agreements
}

func (t *tribe) AddTask(agreementName string, task agreement.Task) serror.SnapError {
	if err := t.canAddTask(task, agreementName); err != nil {
		return err
	}
	msg := &taskMsg{
		LTime:         t.clock.Increment(),
		TaskID:        task.ID,
		StartOnCreate: task.StartOnCreate,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          addTaskMsgType,
	}
	if t.handleAddTask(msg) {
		t.broadcast(addTaskMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) RemoveTask(agreementName string, task agreement.Task) serror.SnapError {
	if err := t.canStartStopRemoveTask(task, agreementName); err != nil {
		return err
	}
	msg := &taskMsg{
		LTime:         t.clock.Increment(),
		TaskID:        task.ID,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          removeTaskMsgType,
	}
	if t.handleRemoveTask(msg) {
		t.broadcast(removeTaskMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) StopTask(agreementName string, task agreement.Task) serror.SnapError {
	if err := t.canStartStopRemoveTask(task, agreementName); err != nil {
		return err
	}
	msg := &taskMsg{
		LTime:         t.clock.Increment(),
		TaskID:        task.ID,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          stopTaskMsgType,
	}
	if t.handleStopTask(msg) {
		t.broadcast(stopTaskMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) StartTask(agreementName string, task agreement.Task) serror.SnapError {
	if err := t.canStartStopRemoveTask(task, agreementName); err != nil {
		return err
	}

	msg := &taskMsg{
		LTime:         t.clock.Increment(),
		TaskID:        task.ID,
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          startTaskMsgType,
	}
	if t.handleStartTask(msg) {
		t.broadcast(startTaskMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) AddAgreement(name string) serror.SnapError {
	if _, ok := t.agreements[name]; ok {
		fields := log.Fields{
			"agreement": name,
		}
		return serror.New(errAgreementAlreadyExists, fields)
	}
	msg := &agreementMsg{
		LTime:         t.clock.Increment(),
		AgreementName: name,
		UUID:          uuid.New(),
		Type:          addAgreementMsgType,
	}
	if t.handleAddAgreement(msg) {
		t.broadcast(addAgreementMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) RemoveAgreement(name string) serror.SnapError {
	if _, ok := t.agreements[name]; !ok {
		fields := log.Fields{
			"Agreement": name,
		}
		return serror.New(errAgreementDoesNotExist, fields)
	}
	msg := &agreementMsg{
		LTime:         t.clock.Increment(),
		AgreementName: name,
		UUID:          uuid.New(),
		Type:          removeAgreementMsgType,
	}
	if t.handleRemoveAgreement(msg) {
		t.broadcast(removeAgreementMsgType, msg, nil)
	}
	return nil
}

func (t *tribe) TaskStateQuery(agreementName string, taskID string) core.TaskState {
	resp := t.taskStateQuery(agreementName, taskID)

	responses := taskStateResponses{}
	for r := range resp.resp {
		responses = append(responses, r)
	}

	return responses.State()
}

// broadcast takes a tribe message type, encodes it for the wire, and queues
// the broadcast. If a notify channel is given, this channel will be closed
// when the broadcast is sent.
func (t *tribe) broadcast(mt msgType, msg interface{}, notify chan<- struct{}) error {
	raw, err := encodeMessage(mt, msg)
	if err != nil {
		return err
	}

	t.broadcasts.QueueBroadcast(&broadcast{
		msg:    raw,
		notify: notify,
	})
	return nil
}

func (t *tribe) taskStateQuery(agreementName string, taskId string) *taskStateQueryResponse {
	timeout := t.getTimeout()
	msg := &taskStateQueryMsg{
		LTime:         t.clock.Increment(),
		AgreementName: agreementName,
		UUID:          uuid.New(),
		Type:          getTaskStateMsgType,
		Addr:          t.memberlist.LocalNode().Addr,
		Port:          t.memberlist.LocalNode().Port,
		TaskID:        taskId,
		Deadline:      time.Now().Add(timeout),
	}

	resp := newStateQueryResponse(len(t.memberlist.Members()), msg)
	t.registerQueryResponse(timeout, resp)
	t.broadcast(msg.Type, msg, nil)

	return resp
}

func (t *tribe) registerQueryResponse(timeout time.Duration, resp *taskStateQueryResponse) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, ok := t.taskStateResponses[resp.uuid]; ok {
		panic("not ok")
	}
	t.taskStateResponses[resp.uuid] = resp

	time.AfterFunc(timeout, func() {
		t.mutex.Lock()
		delete(t.taskStateResponses, resp.uuid)
		resp.Close()
		t.mutex.Unlock()
	})
}

func (t *tribe) joinAgreement(msg *agreementMsg) serror.SnapError {
	if err := t.canJoinAgreement(msg.Agreement(), msg.MemberName); err != nil {
		return err
	}
	// add plugin agreement to the member
	if t.agreements[msg.Agreement()].PluginAgreement != nil {
		t.members[msg.MemberName].PluginAgreement = t.agreements[msg.Agreement()].PluginAgreement
	}
	t.members[msg.MemberName].TaskAgreements[msg.Agreement()] = t.agreements[msg.Agreement()].TaskAgreement

	// update the agreements membership
	t.agreements[msg.Agreement()].Members[msg.MemberName] = t.members[msg.MemberName]

	// get plugins and tasks if this is the node joining
	if msg.MemberName == t.memberlist.LocalNode().Name {
		go func(a *agreement.Agreement) {
			for _, p := range a.PluginAgreement.Plugins {
				ptype, _ := core.ToPluginType(p.TypeName())
				work := worker.PluginRequest{
					Plugin: agreement.Plugin{
						Name_:    p.Name(),
						Version_: p.Version(),
						Type_:    ptype,
					},
					RequestType: worker.PluginLoadedType,
				}
				t.pluginWorkQueue <- work
			}

			for _, tsk := range a.TaskAgreement.Tasks {
				state := t.TaskStateQuery(msg.Agreement(), tsk.ID)
				startOnCreate := false
				if state == core.TaskSpinning || state == core.TaskFiring {
					startOnCreate = true
				}
				work := worker.TaskRequest{
					Task: worker.Task{
						ID:            tsk.ID,
						StartOnCreate: startOnCreate,
					},
					RequestType: worker.TaskCreatedType,
				}
				t.taskWorkQueue <- work
			}
		}(t.agreements[msg.Agreement()])
	}
	return nil
}

func (t *tribe) leaveAgreement(msg *agreementMsg) serror.SnapError {
	if err := t.canLeaveAgreement(msg.Agreement(), msg.MemberName); err != nil {
		return err
	}

	delete(t.agreements[msg.AgreementName].Members, msg.MemberName)
	t.members[msg.MemberName].PluginAgreement = nil
	if _, ok := t.members[msg.MemberName].TaskAgreements[msg.Agreement()]; ok {
		delete(t.members[msg.MemberName].TaskAgreements, msg.Agreement())
	}

	return nil
}

func (t *tribe) canLeaveAgreement(agreementName, memberName string) serror.SnapError {
	fields := log.Fields{
		"member-name": memberName,
		"agreement":   agreementName,
	}
	if _, ok := t.agreements[agreementName]; !ok {
		t.logger.WithFields(fields).Debugln(errAgreementDoesNotExist)
		return serror.New(errAgreementDoesNotExist, fields)
	}
	m, ok := t.members[memberName]
	if !ok {
		t.logger.WithFields(fields).Debugln(errUnknownMember)
		return serror.New(errUnknownMember, fields)
	}
	if m.PluginAgreement == nil {
		t.logger.WithFields(fields).Debugln(errNotAMember)
		return serror.New(errNotAMember, fields)
	}
	return nil
}

func (t *tribe) canJoinAgreement(agreementName, memberName string) serror.SnapError {
	fields := log.Fields{
		"member-name": memberName,
		"agreement":   agreementName,
	}
	if _, ok := t.agreements[agreementName]; !ok {
		t.logger.WithFields(fields).Debugln(errAgreementDoesNotExist)
		return serror.New(errAgreementDoesNotExist, fields)
	}
	m, ok := t.members[memberName]
	if !ok {
		t.logger.WithFields(fields).Debugln(errUnknownMember)
		return serror.New(errUnknownMember, fields)

	}
	if m.PluginAgreement != nil && len(m.PluginAgreement.Plugins) > 0 {
		t.logger.WithFields(fields).Debugln(errAlreadyMemberOfPluginAgreement)
		return serror.New(errAlreadyMemberOfPluginAgreement, fields)
	}
	return nil
}

func (t *tribe) canAddTask(task agreement.Task, agreementName string) serror.SnapError {
	fields := log.Fields{
		"agreement": agreementName,
		"task-id":   task.ID,
	}
	a, ok := t.agreements[agreementName]
	if !ok {
		t.logger.WithFields(fields).Debugln(errAgreementDoesNotExist)
		return serror.New(errAgreementDoesNotExist, fields)
	}
	if ok, _ := a.TaskAgreement.Tasks.Contains(task); ok {
		t.logger.WithFields(fields).Debugln(errTaskAlreadyExists)
		return serror.New(errTaskAlreadyExists, fields)
	}
	return nil
}

func (t *tribe) canStartStopRemoveTask(task agreement.Task, agreementName string) serror.SnapError {
	fields := log.Fields{
		"agreement": agreementName,
		"task-id":   task.ID,
	}
	a, ok := t.agreements[agreementName]
	if !ok {
		t.logger.WithFields(fields).Debugln(errAgreementDoesNotExist)
		return serror.New(errAgreementDoesNotExist, fields)
	}
	if ok, _ := a.TaskAgreement.Tasks.Contains(task); !ok {
		t.logger.WithFields(fields).Debugln(errTaskDoesNotExist)
		return serror.New(errTaskDoesNotExist, fields)
	}
	return nil
}

func (t *tribe) isMemberOfAgreement(name string) bool {
	fields := log.Fields{
		"agreement": name,
		"_block":    "isMemberOfAgreement",
	}
	a, ok := t.agreements[name]
	if !ok {
		t.logger.WithFields(fields).Debugln(errAgreementDoesNotExist)
		return false
	}
	if _, ok := a.Members[t.memberlist.LocalNode().Name]; !ok {
		t.logger.WithFields(fields).Debugln(errNotAMember)
		return false
	}
	return true
}

func (t *tribe) isDuplicate(msg msg) bool {
	logger := t.logger.WithFields(log.Fields{
		"event-clock": msg.Time(),
		"event":       msg.GetType().String(),
		"event-uuid":  msg.ID(),
		"clock":       t.clock.Time(),
		"agreement":   msg.Agreement(),
	})
	// is the message old
	if t.clock.Time() > LTime(len(t.msgBuffer)) &&
		msg.Time() < t.clock.Time()-LTime(len(t.msgBuffer)) {
		logger.Debugln("old message")
		return true
	}

	// have we seen it
	idx := msg.Time() % LTime(len(t.msgBuffer))
	seen := t.msgBuffer[idx]
	if seen != nil && seen.ID() == msg.ID() {
		logger.Debugln("duplicate message")
		return true
	}
	return false
}

func (t *tribe) getTimeout() time.Duration {
	// query duration - gossip interval * timeout mult * log(n+1)
	return time.Duration(t.config.MemberlistConfig.GossipInterval * 5 * time.Duration(math.Ceil(math.Log10(float64(len(t.memberlist.Members())+1)))))
}
