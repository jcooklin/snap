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
	"net"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/memberlist"

	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/mgmt/tribe/agreement"
	"github.com/intelsdi-x/snap/mgmt/tribe/worker"
)

func (t *tribe) handleRemovePlugin(msg *pluginMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.Agreement()]; ok {
		if t.agreements[msg.AgreementName].PluginAgreement.Remove(msg.Plugin) {
			t.processIntents()
			if t.pluginCatalog != nil {
				_, err := t.pluginCatalog.Unload(msg.Plugin)
				if err != nil {
					t.logger.WithFields(log.Fields{
						"_block":         "handle-remove-plugin",
						"plugin-name":    msg.Plugin.Name(),
						"plugin-type":    msg.Plugin.TypeName(),
						"plugin-version": msg.Plugin.Version(),
					}).Error(err)
				}
			}
			return true
		}
	}

	t.addPluginIntent(msg)
	return true
}

func (t *tribe) handleAddPlugin(msg *pluginMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.AgreementName]; ok {
		if t.agreements[msg.AgreementName].PluginAgreement.Add(msg.Plugin) {

			ptype, _ := core.ToPluginType(msg.Plugin.TypeName())
			work := worker.PluginRequest{
				Plugin: agreement.Plugin{
					Name_:    msg.Plugin.Name(),
					Version_: msg.Plugin.Version(),
					Type_:    ptype,
				},
				RequestType: worker.PluginLoadedType,
			}
			t.pluginWorkQueue <- work

			t.processIntents()
			return true
		}
	}

	t.addPluginIntent(msg)
	return true
}

func (t *tribe) handleAddTask(msg *taskMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.AgreementName]; ok {
		if t.agreements[msg.AgreementName].TaskAgreement.Add(agreement.Task{ID: msg.TaskID}) {

			work := worker.TaskRequest{
				Task: worker.Task{
					ID:            msg.TaskID,
					StartOnCreate: msg.StartOnCreate,
				},
				RequestType: worker.TaskCreatedType,
			}
			t.taskWorkQueue <- work

			t.processIntents()
			return true
		}
	}

	t.addTaskIntent(msg)
	return true
}

func (t *tribe) handleRemoveTask(msg *taskMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.Agreement()]; ok {
		if t.agreements[msg.AgreementName].TaskAgreement.Remove(agreement.Task{ID: msg.TaskID}) {

			work := worker.TaskRequest{
				Task: worker.Task{
					ID: msg.TaskID,
				},
				RequestType: worker.TaskRemovedType,
			}
			t.taskWorkQueue <- work

			t.processIntents()
			return true
		}
	}

	t.addTaskIntent(msg)
	return true
}

func (t *tribe) handleStartTask(msg *taskMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.Agreement()]; ok {

		if ok := t.taskStartStopCache.put(msg, t.getTimeout()); !ok {
			// A cache entry exists; return and do not broadcast event again
			return false
		}

		work := worker.TaskRequest{
			Task: worker.Task{
				ID: msg.TaskID,
			},
			RequestType: worker.TaskStartedType,
		}
		t.taskWorkQueue <- work

		return true
	}

	return true
}

func (t *tribe) handleStopTask(msg *taskMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.Agreement()]; ok {

		if ok := t.taskStartStopCache.put(msg, t.getTimeout()); !ok {
			// A cache entry exists; return and do not broadcast event again
			return false
		}

		work := worker.TaskRequest{
			Task: worker.Task{
				ID: msg.TaskID,
			},
			RequestType: worker.TaskStoppedType,
		}
		t.taskWorkQueue <- work

		return true
	}

	return true
}

func (t *tribe) handleMemberJoin(n *memberlist.Node) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.members[n.Name]; !ok {
		t.members[n.Name] = agreement.NewMember(n)
		t.members[n.Name].Tags = t.decodeTags(n.Meta)
	}
	t.processIntents()
}

func (t *tribe) handleMemberLeave(n *memberlist.Node) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if m, ok := t.members[n.Name]; ok {
		if m.PluginAgreement != nil {
			delete(t.agreements[m.PluginAgreement.Name].Members, n.Name)
		}
		for k := range m.TaskAgreements {
			delete(t.agreements[k].Members, n.Name)
		}
		delete(t.members, n.Name)
	}
}

func (t *tribe) handleMemberUpdate(n *memberlist.Node) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if _, ok := t.members[n.Name]; ok {
		t.members[n.Name].Tags = t.decodeTags(n.Meta)
	}
}

func (t *tribe) handleAddAgreement(msg *agreementMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	// add msg to seen buffer
	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	// add agreement
	if _, ok := t.agreements[msg.AgreementName]; !ok {
		t.agreements[msg.AgreementName] = agreement.New(msg.AgreementName)
		t.processIntents()
		return true
	}
	t.addAgreementIntent(msg)
	return true
}

func (t *tribe) handleRemoveAgreement(msg *agreementMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	// add msg to seen buffer
	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if _, ok := t.agreements[msg.AgreementName]; ok {
		delete(t.agreements, msg.AgreementName)
		t.processIntents()
		// TODO consider removing any intents that involve this agreement
		return true
	}

	return true
}

func (t *tribe) handleJoinAgreement(msg *agreementMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if err := t.joinAgreement(msg); err == nil {
		t.processIntents()
		return true
	}

	t.addAgreementIntent(msg)
	return true
}

func (t *tribe) handleLeaveAgreement(msg *agreementMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if err := t.leaveAgreement(msg); err == nil {
		t.processIntents()
		return true
	}

	t.addAgreementIntent(msg)

	return true
}

func (t *tribe) handleTaskStateQuery(msg *taskStateQueryMsg) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// update the clock if newer
	t.clock.Update(msg.LTime)

	if t.isDuplicate(msg) {
		return false
	}

	t.msgBuffer[msg.LTime%LTime(len(t.msgBuffer))] = msg

	if time.Now().After(msg.Deadline) {
		t.logger.WithFields(log.Fields{
			"_block":   "handleStateQuery",
			"deadline": msg.Deadline,
			"ltime":    msg.LTime,
		}).Warn("deadline passed for task state query")
		return false
	}

	if !t.isMemberOfAgreement(msg.Agreement()) {
		// we are not a member of the agreement
		return true
	}

	resp := taskStateQueryResponseMsg{
		LTime: msg.LTime,
		UUID:  msg.UUID,
		From:  t.memberlist.LocalNode().Name,
	}

	tsk, err := t.taskManager.GetTask(msg.TaskID)
	if err != nil {
		t.logger.WithFields(log.Fields{
			"_block":  "handleStateQuery",
			"err":     err,
			"task-id": msg.TaskID,
			"msg-id":  msg.UUID,
		}).Error("failed to get task state")
		return true
	}

	resp.State = tsk.State()

	// Format the response
	raw, err := encodeMessage(taskStateQueryResponseMsgType, &resp)
	if err != nil {
		t.logger.WithFields(log.Fields{
			"_block": "handleStateQuery",
			"err":    err,
		}).Error("failed to encode message")
		return true
	}

	// Check the size limit
	if len(raw) > TaskStateQueryResponseSizeLimit {
		t.logger.WithFields(log.Fields{
			"_block":     "handleStateQuery",
			"err":        err,
			"size-limit": TaskStateQueryResponseSizeLimit,
			"msg-size":   len(raw),
		}).Error("msg exceeds size limit", TaskStateQueryResponseSizeLimit)
		return true
	}

	// Send the response
	addr := net.UDPAddr{IP: msg.Addr, Port: int(msg.Port)}
	if err := t.memberlist.SendTo(&addr, raw); err != nil {
		t.logger.WithFields(log.Fields{
			"_block":      "handleStateQuery",
			"remote-addr": msg.Addr,
			"remote-port": msg.Port,
			"err":         err,
		}).Error("failed to send task state reply")
	}

	return true
}
