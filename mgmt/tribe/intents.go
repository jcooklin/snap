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
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/mgmt/tribe/agreement"
	"github.com/intelsdi-x/snap/mgmt/tribe/worker"
)

func (t *tribe) addIntent(m msg) {
	if t.counters != nil {
		defer t.counters.incIntentCounter(m.GetType())
	}
	t.intentBuffer = append(t.intentBuffer, m)
}

func (t *tribe) removeIntent(idx int) {
	m := t.intentBuffer[idx]
	if t.counters != nil {
		defer t.counters.decIntentCounter(m.GetType())
	}
	t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
}

func (t *tribe) processIntents() {
	for {
		if t.processAddPluginIntents() &&
			t.processRemovePluginIntents() &&
			t.processAddAgreementIntents() &&
			t.processRemoveAgreementIntents() &&
			t.processJoinAgreementIntents() &&
			t.processLeaveAgreementIntents() &&
			t.processAddTaskIntents() &&
			t.processRemoveTaskIntents() {
			return
		}
	}
}

func (t *tribe) processAddPluginIntents() bool {
	for idx, v := range t.intentBuffer {
		if v.GetType() == addPluginMsgType {
			intent := v.(*pluginMsg)
			if _, ok := t.agreements[intent.AgreementName]; ok {
				if ok, _ := t.agreements[intent.AgreementName].PluginAgreement.Plugins.Contains(intent.Plugin); !ok {
					t.agreements[intent.AgreementName].PluginAgreement.Plugins = append(t.agreements[intent.AgreementName].PluginAgreement.Plugins, intent.Plugin)
					// t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
					t.removeIntent(idx)
					ptype, _ := core.ToPluginType(intent.Plugin.TypeName())
					work := worker.PluginRequest{
						Plugin: agreement.Plugin{
							Name_:    intent.Plugin.Name(),
							Version_: intent.Plugin.Version(),
							Type_:    ptype,
						},
						RequestType: worker.PluginLoadedType,
					}
					t.pluginWorkQueue <- work

					return false
				}
			}
		}
	}
	return true
}

func (t *tribe) processRemovePluginIntents() bool {
	for k, v := range t.intentBuffer {
		if v.GetType() == removePluginMsgType {
			intent := v.(*pluginMsg)
			if a, ok := t.agreements[intent.AgreementName]; ok {
				if ok, idx := a.PluginAgreement.Plugins.Contains(intent.Plugin); ok {
					a.PluginAgreement.Plugins = append(a.PluginAgreement.Plugins[:idx], a.PluginAgreement.Plugins[idx+1:]...)
					// t.intentBuffer = append(t.intentBuffer[:k], t.intentBuffer[k+1:]...)
					t.removeIntent(k)
					return false
				}
			}
		}
	}
	return true
}

func (t *tribe) processAddTaskIntents() bool {
	for idx, v := range t.intentBuffer {
		if v.GetType() == addTaskMsgType {
			intent := v.(*taskMsg)
			if a, ok := t.agreements[intent.AgreementName]; ok {
				if ok, _ := a.TaskAgreement.Tasks.Contains(agreement.Task{ID: intent.TaskID}); !ok {
					a.TaskAgreement.Tasks = append(a.TaskAgreement.Tasks, agreement.Task{ID: intent.TaskID})
					// t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
					t.removeIntent(idx)
					work := worker.TaskRequest{
						Task: worker.Task{
							ID:            intent.TaskID,
							StartOnCreate: intent.StartOnCreate,
						},
						RequestType: worker.TaskCreatedType,
					}
					t.taskWorkQueue <- work

					return false
				}
			}
		}
	}
	return true
}

func (t *tribe) processRemoveTaskIntents() bool {
	for k, v := range t.intentBuffer {
		if v.GetType() == removeTaskMsgType {
			intent := v.(*taskMsg)
			if _, ok := t.agreements[intent.AgreementName]; ok {
				if ok, idx := t.agreements[intent.AgreementName].TaskAgreement.Tasks.Contains(agreement.Task{ID: intent.TaskID}); ok {
					t.agreements[intent.AgreementName].TaskAgreement.Tasks = append(t.agreements[intent.AgreementName].TaskAgreement.Tasks[:idx], t.agreements[intent.AgreementName].TaskAgreement.Tasks[idx+1:]...)
					// t.intentBuffer = append(t.intentBuffer[:k], t.intentBuffer[k+1:]...)
					t.removeIntent(k)
					return false
				}
			}
		}
	}
	return true
}

func (t *tribe) processAddAgreementIntents() bool {
	for idx, v := range t.intentBuffer {
		if v.GetType() == addAgreementMsgType {
			intent := v.(*agreementMsg)
			if _, ok := t.agreements[intent.AgreementName]; !ok {
				t.agreements[intent.AgreementName] = agreement.New(intent.AgreementName)
				// t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
				t.removeIntent(idx)
				return false
			}
		}
	}
	return true
}

func (t *tribe) processRemoveAgreementIntents() bool {
	for k, v := range t.intentBuffer {
		if v.GetType() == removeAgreementMsgType {
			intent := v.(*agreementMsg)
			if _, ok := t.agreements[intent.Agreement()]; ok {
				delete(t.agreements, intent.Agreement())
				// t.intentBuffer = append(t.intentBuffer[:k], t.intentBuffer[k+1:]...)
				t.removeIntent(k)
				return false
			}
		}
	}
	return true
}

func (t *tribe) processJoinAgreementIntents() bool {
	for idx, v := range t.intentBuffer {
		if v.GetType() == joinAgreementMsgType {
			intent := v.(*agreementMsg)
			if _, ok := t.members[intent.MemberName]; ok {
				if _, ok := t.agreements[intent.AgreementName]; ok {
					err := t.joinAgreement(intent)
					if err == nil {
						// t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
						t.removeIntent(idx)
					}
					return false
				}
			}
		}
	}
	return true
}

func (t *tribe) processLeaveAgreementIntents() bool {
	for idx, v := range t.intentBuffer {
		if v.GetType() == joinAgreementMsgType {
			intent := v.(*agreementMsg)
			if _, ok := t.members[intent.MemberName]; ok {
				if _, ok := t.agreements[intent.AgreementName]; ok {
					if _, ok := t.agreements[intent.AgreementName].Members[intent.MemberName]; ok {
						err := t.leaveAgreement(intent)
						if err == nil {
							// t.intentBuffer = append(t.intentBuffer[:idx], t.intentBuffer[idx+1:]...)
							t.removeIntent(idx)
						}
						return false
					}
				}
			}
		}
	}
	return true
}

func (t *tribe) addPluginIntent(msg *pluginMsg) bool {
	t.logger.WithFields(log.Fields{
		"event-clock": msg.LTime,
		"agreement":   msg.AgreementName,
		"type":        msg.Type.String(),
		"plugin": fmt.Sprintf("%v:%v:%v",
			msg.Plugin.TypeName(),
			msg.Plugin.Name(),
			msg.Plugin.Version()),
	}).Debugln("out of order message")
	// t.intentBuffer = append(t.intentBuffer, msg)
	t.addIntent(msg)
	return true
}

func (t *tribe) addAgreementIntent(m msg) bool {
	t.logger.WithFields(log.Fields{
		"event-clock": m.Time(),
		"agreement":   m.Agreement(),
		"type":        m.GetType().String(),
	}).Debugln("out of order message")
	// t.intentBuffer = append(t.intentBuffer, m)
	t.addIntent(m)
	return true
}

func (t *tribe) addTaskIntent(m *taskMsg) bool {
	t.logger.WithFields(log.Fields{
		"event-clock": m.Time(),
		"agreement":   m.Agreement(),
		"type":        m.GetType().String(),
		"task-id":     m.TaskID,
	}).Debugln("Out of order msg")
	// t.intentBuffer = append(t.intentBuffer, m)
	t.addIntent(m)
	return true
}
