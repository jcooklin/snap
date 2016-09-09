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
	MyName    string
	MyType    string
	MyVersion int
}

func (m MockLoadedPlugin) Name() string       { return m.MyName }
func (m MockLoadedPlugin) TypeName() string   { return m.MyType }
func (m MockLoadedPlugin) Version() int       { return m.MyVersion }
func (m MockLoadedPlugin) Plugin() string     { return "" }
func (m MockLoadedPlugin) IsSigned() bool     { return false }
func (m MockLoadedPlugin) Status() string     { return "" }
func (m MockLoadedPlugin) PluginPath() string { return "" }
func (m MockLoadedPlugin) LoadedTimestamp() *time.Time {
	t := time.Date(2016, time.September, 6, 0, 0, 0, 0, time.UTC)
	return &t
}
func (m MockLoadedPlugin) Policy() *cpolicy.ConfigPolicy { return cpolicy.New() }

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
		MockLoadedPlugin{MyName: "foo", MyType: "collector", MyVersion: 2},
		MockLoadedPlugin{MyName: "bar", MyType: "publisher", MyVersion: 3},
		MockLoadedPlugin{MyName: "foo", MyType: "collector", MyVersion: 4},
		MockLoadedPlugin{MyName: "baz", MyType: "publisher", MyVersion: 5},
		MockLoadedPlugin{MyName: "foo", MyType: "processor", MyVersion: 6},
		MockLoadedPlugin{MyName: "foobar", MyType: "processor", MyVersion: 1},
	}
}
func (m MockManagesMetrics) AvailablePlugins() []core.AvailablePlugin {
	return []core.AvailablePlugin{
		MockLoadedPlugin{MyName: "foo", MyType: "collector", MyVersion: 2},
		MockLoadedPlugin{MyName: "bar", MyType: "publisher", MyVersion: 3},
		MockLoadedPlugin{MyName: "foo", MyType: "collector", MyVersion: 4},
		MockLoadedPlugin{MyName: "baz", MyType: "publisher", MyVersion: 5},
		MockLoadedPlugin{MyName: "foo", MyType: "processor", MyVersion: 6},
		MockLoadedPlugin{MyName: "foobar", MyType: "processor", MyVersion: 1},
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
        "version": 2,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/2"
      },
      {
        "name": "bar",
        "version": 3,
        "type": "publisher",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/publisher/bar/3"
      },
      {
        "name": "foo",
        "version": 4,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/4"
      },
      {
        "name": "baz",
        "version": 5,
        "type": "publisher",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/publisher/baz/5"
      },
      {
        "name": "foo",
        "version": 6,
        "type": "processor",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/processor/foo/6"
      },
      {
        "name": "foobar",
        "version": 1,
        "type": "processor",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/processor/foobar/1"
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
        "version": 2,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/2"
      },
      {
        "name": "foo",
        "version": 4,
        "type": "collector",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/collector/foo/4"
      }
    ]
  }
}`

	GET_PLUGINS_RESPONSE_TYPE_NAME = `{
  "meta": {
    "code": 200,
    "message": "Plugin list returned",
    "type": "plugin_list_returned",
    "version": 1
  },
  "body": {
    "loaded_plugins": [
      {
        "name": "bar",
        "version": 3,
        "type": "publisher",
        "signed": false,
        "status": "",
        "loaded_timestamp": 1473120000,
        "href": "http://localhost:%d/v1/plugins/publisher/bar/3"
      }
    ]
  }
}`

	GET_PLUGINS_RESPONSE_TYPE_NAME_VERSION = `{
  "meta": {
    "code": 200,
    "message": "Plugin returned",
    "type": "plugin_returned",
    "version": 1
  },
  "body": {
    "name": "bar",
    "version": 3,
    "type": "publisher",
    "signed": false,
    "status": "",
    "loaded_timestamp": 1473120000,
    "href": "http://localhost:%d/v1/plugins/publisher/bar/3"
  }
}`

	GET_PLUGIN_CONFIG_ITEM = `{
  "meta": {
    "code": 200,
    "message": "Plugin returned",
    "type": "plugin_returned",
    "version": 1
  },
  "body": {
    "name": "bar",
    "version": 3,
    "type": "publisher",
    "signed": false,
    "status": "",
    "loaded_timestamp": 1473120000,
    "href": "http://localhost:%d/v1/plugins/publisher/bar/3"
  }
}`

	GET_METRICS_RESPONSE = `{
  "meta": {
    "code": 200,
    "message": "Metrics returned",
    "type": "metrics_returned",
    "version": 1
  },
  "body": []
}`
)
