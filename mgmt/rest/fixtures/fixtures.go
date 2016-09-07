// + build legacy small medium large

/*
http://www.apache.org/licenses/LICENSE-2.0.txt

Copyright 2016 Intel Corporation

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

package fixtures

import (
	"time"

	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/serror"
	"github.com/intelsdi-x/snap/pkg/schedule"
	"github.com/intelsdi-x/snap/scheduler/wmap"
)

type MockLoadedPlugin struct {
	MyName string
	MyType string
}

func (m MockLoadedPlugin) Name() string       { return m.MyName }
func (m MockLoadedPlugin) TypeName() string   { return m.MyType }
func (m MockLoadedPlugin) Version() int       { return 0 }
func (m MockLoadedPlugin) Plugin() string     { return "" }
func (m MockLoadedPlugin) IsSigned() bool     { return false }
func (m MockLoadedPlugin) Status() string     { return "" }
func (m MockLoadedPlugin) PluginPath() string { return "" }
func (m MockLoadedPlugin) LoadedTimestamp() *time.Time {
	t := time.Date(2016, time.September, 6, 0, 0, 0, 0, time.UTC)
	return &t
}
func (m MockLoadedPlugin) Policy() *cpolicy.ConfigPolicy { return nil }

// have my mock object also support AvailablePlugin
func (m MockLoadedPlugin) HitCount() int { return 0 }
func (m MockLoadedPlugin) LastHit() time.Time {
	return time.Now()
}
func (m MockLoadedPlugin) ID() uint32 { return 0 }

type MockManagesMetrics struct{}

func (m MockManagesMetrics) MetricCatalog() ([]core.CatalogedMetric, error) {
	return nil, nil
}
func (m MockManagesMetrics) FetchMetrics(core.Namespace, int) ([]core.CatalogedMetric, error) {
	return nil, nil
}
func (m MockManagesMetrics) GetMetricVersions(core.Namespace) ([]core.CatalogedMetric, error) {
	return nil, nil
}
func (m MockManagesMetrics) GetMetric(core.Namespace, int) (core.CatalogedMetric, error) {
	return nil, nil
}
func (m MockManagesMetrics) Load(*core.RequestedPlugin) (core.CatalogedPlugin, serror.SnapError) {
	return nil, nil
}
func (m MockManagesMetrics) Unload(core.Plugin) (core.CatalogedPlugin, serror.SnapError) {
	return nil, nil
}

func (m MockManagesMetrics) PluginCatalog() core.PluginCatalog {
	return []core.CatalogedPlugin{
		MockLoadedPlugin{MyName: "foo", MyType: "collector"},
		MockLoadedPlugin{MyName: "bar", MyType: "publisher"},
		MockLoadedPlugin{MyName: "foo", MyType: "collector"},
		MockLoadedPlugin{MyName: "baz", MyType: "publisher"},
		MockLoadedPlugin{MyName: "foo", MyType: "processor"},
		MockLoadedPlugin{MyName: "foobar", MyType: "processor"},
	}
}
func (m MockManagesMetrics) AvailablePlugins() []core.AvailablePlugin {
	return []core.AvailablePlugin{
		MockLoadedPlugin{MyName: "foo", MyType: "collector"},
		MockLoadedPlugin{MyName: "bar", MyType: "publisher"},
		MockLoadedPlugin{MyName: "foo", MyType: "collector"},
		MockLoadedPlugin{MyName: "baz", MyType: "publisher"},
		MockLoadedPlugin{MyName: "foo", MyType: "processor"},
		MockLoadedPlugin{MyName: "foobar", MyType: "processor"},
	}
}
func (m MockManagesMetrics) GetAutodiscoverPaths() []string {
	return nil
}

////////////////
type mockTask struct{}

func (t *mockTask) ID() string                                { return "" }
func (t *mockTask) State() core.TaskState                     { return core.TaskSpinning }
func (t *mockTask) HitCount() uint                            { return 0 }
func (t *mockTask) GetName() string                           { return "" }
func (t *mockTask) SetName(string)                            { return }
func (t *mockTask) SetID(string)                              { return }
func (t *mockTask) MissedCount() uint                         { return 0 }
func (t *mockTask) FailedCount() uint                         { return 0 }
func (t *mockTask) LastFailureMessage() string                { return "" }
func (t *mockTask) LastRunTime() *time.Time                   { return nil }
func (t *mockTask) CreationTime() *time.Time                  { return nil }
func (t *mockTask) DeadlineDuration() time.Duration           { return 0 }
func (t *mockTask) SetDeadlineDuration(time.Duration)         { return }
func (t *mockTask) SetTaskID(id string)                       { return }
func (t *mockTask) SetStopOnFailure(int)                      { return }
func (t *mockTask) GetStopOnFailure() int                     { return 0 }
func (t *mockTask) Option(...core.TaskOption) core.TaskOption { return core.TaskDeadlineDuration(0) }
func (t *mockTask) WMap() *wmap.WorkflowMap                   { return nil }
func (t *mockTask) Schedule() schedule.Schedule               { return nil }
func (t *mockTask) MaxFailures() int                          { return 10 }

type MockManagesTasks struct{}

func (m *MockManagesTasks) GetTask(id string) (core.Task, error) { return &mockTask{}, nil }
func (m *MockManagesTasks) CreateTask(sch schedule.Schedule, wmap *wmap.WorkflowMap, start bool, opts ...core.TaskOption) (core.Task, core.TaskErrors) {
	return nil, nil
}
func (m *MockManagesTasks) GetTasks() map[string]core.Task         { return nil }
func (m *MockManagesTasks) StartTask(id string) []serror.SnapError { return nil }
func (m *MockManagesTasks) StopTask(id string) []serror.SnapError  { return nil }
func (m *MockManagesTasks) RemoveTask(id string) error             { return nil }
func (m *MockManagesTasks) WatchTask(id string, handler core.TaskWatcherHandler) (core.TaskWatcherCloser, error) {
	return nil, nil
}
func (m *MockManagesTasks) EnableTask(id string) (core.Task, error) { return nil, nil }

////////////////

const (
	GET_PLUGINS_RESPONSE = `{
  "meta": {
    "code": 200,
    "message": "Plugin list returned",
    "type": "plugin_list_returned",
    "version": 1
  },
  "body": {
    "loaded_plugins": [
      {
        "name": "foo",
        "version": 0,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/0"
      },
      {
        "name": "bar",
        "version": 0,
        "type": "publisher",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/publisher/bar/0"
      },
      {
        "name": "foo",
        "version": 0,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/0"
      },
      {
        "name": "baz",
        "version": 0,
        "type": "publisher",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/publisher/baz/0"
      },
      {
        "name": "foo",
        "version": 0,
        "type": "processor",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/processor/foo/0"
      },
      {
        "name": "foobar",
        "version": 0,
        "type": "processor",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/processor/foobar/0"
      }
    ]
  }
}`

	GET_PLUGINS_RESPONSE_TYPE = `{
  "meta": {
    "code": 200,
    "message": "Plugin list returned",
    "type": "plugin_list_returned",
    "version": 1
  },
  "body": {
    "loaded_plugins": [
      {
        "name": "foo",
        "version": 0,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/0"
      },
      {
        "name": "foo",
        "version": 0,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/0"
      }
    ]
  }
}`
)
