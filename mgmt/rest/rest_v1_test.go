// +build medium

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

package rest

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	log "github.com/Sirupsen/logrus"
	"github.com/intelsdi-x/snap/mgmt/rest/fixtures"
)

var (
	LOG_LEVEL         = log.DebugLevel
	SNAP_PATH         = os.ExpandEnv(os.Getenv("SNAP_PATH"))
	MOCK_PLUGIN_PATH1 = SNAP_PATH + "/plugin/snap-plugin-collector-mock1"
)

type restAPIInstance struct {
	port   int
	server *Server
}

func startV1API(cfg *mockConfig) *restAPIInstance {
	// Start a REST API to talk to
	log.SetLevel(LOG_LEVEL)
	mockMetricManager := &fixtures.MockManagesMetrics{}
	r, _ := New(cfg.RestAPI)
	r.BindMetricManager(mockMetricManager)
	mockTaskManager := &fixtures.MockManagesTasks{}
	//TODO bind mock task manager r.BindTaskManager(s)
	r.BindTaskManager(mockTaskManager)
	//TODO bind mock config manager r.BindConfigManager(c.Config)
	go func(ch <-chan error) {
		// Block on the error channel. Will return exit status 1 for an error or
		// just return if the channel closes.
		err, ok := <-ch
		if !ok {
			return
		}
		log.Fatal(err)
	}(r.Err())
	r.SetAddress("127.0.0.1:0")
	r.Start()
	return &restAPIInstance{
		port:   r.Port(),
		server: r,
	}
}

func TestV1(t *testing.T) {
	r := startV1API(getDefaultMockConfig())
	Convey("Test REST API V1", t, func() {
		//////////TEST-PLUGIN-ROUTES/////////////////
		// Convey("Get plugins - v1/plugins", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/plugins", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE, r.port, r.port,
		// 			r.port, r.port, r.port, r.port),
		// 		ShouldResemble,
		// 		string(body))
		// })
		// Convey("Get plugins - v1/plugins/:type", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/plugins/collector", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE_TYPE, r.port, r.port),
		// 		ShouldResemble,
		// 		string(body))
		// })
		// Convey("Get plugins - v1/plugins/:type:name", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/plugins/publisher/bar", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE_TYPE_NAME, r.port),
		// 		ShouldResemble,
		// 		string(body))
		// })
		// Convey("Get plugins - v1/plugins/:type:name:version", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/plugins/publisher/bar/3", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE_TYPE_NAME_VERSION, r.port),
		// 		ShouldResemble,
		// 		string(body))
		// })

		Convey("Post plugins - v1/plugins/:type:name", func(c C) {
			f, err := os.Open(MOCK_PLUGIN_PATH1)
			defer f.Close()
			So(err, ShouldBeNil)

			// We create a pipe so that we can write the file in multipart
			// format and read it in to the body of the http request
			reader, writer := io.Pipe()
			mwriter := multipart.NewWriter(writer)
			bufin := bufio.NewReader(f)

			// A go routine is needed since we must write the multipart file
			// to the pipe so we can read from it in the http call
			go func() {
				part, err := mwriter.CreateFormFile("snap-plugins", "mock")
				c.So(err, ShouldBeNil)
				bufin.WriteTo(part)
				mwriter.Close()
				writer.Close()
			}()

			resp1, err1 := http.Post(
				fmt.Sprintf("http://localhost:%d/v1/plugins", r.port),
				mwriter.FormDataContentType(), reader)
			So(err1, ShouldBeNil)
			So(resp1.StatusCode, ShouldEqual, 201)
		})
		Convey("Delete plugins - v1/plugins/:type:name:version", func() {})

		Convey("Get plugin config items - v1/plugins/:type:name:version:config", func() {
			resp, err := http.Get(
				fmt.Sprintf("http://localhost:%d/v1/plugins/publisher/bar/3/", r.port))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, 200)
			body, err := ioutil.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			So(
				fmt.Sprintf(fixtures.GET_PLUGIN_CONFIG_ITEM, r.port),
				ShouldResemble,
				string(body))
		})

		////////////TEST-METRIC-ROUTES/////////////////

		// Convey("Get metric items - v1/metrics", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/metrics", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	//fmt.Print(string(body))
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_METRICS_RESPONSE),
		// 		ShouldResemble,
		// 		string(body))
		// })
		// Convey("Get metric items - v1/metrics/*namespace", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/metrics/", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	//fmt.Print(string(body))
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_METRICS_RESPONSE), //will be same as above
		// 		ShouldResemble,
		// 		string(body))
		// })

		//////////TEST-TASK-ROUTES/////////////////

		//Needs something to do with MockManagesTasks in fixtures.go

		// Convey("Get tasks - v1/tasks", func() {
		// 	resp, err := http.Get(
		// 		fmt.Sprintf("http://localhost:%d/v1/tasks", r.port))
		// 	So(err, ShouldBeNil)
		// 	So(resp.StatusCode, ShouldEqual, 200)
		// 	body, err := ioutil.ReadAll(resp.Body)
		// 	So(err, ShouldBeNil)
		// 	So(
		// 		fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE, r.port, r.port,
		// 			r.port, r.port, r.port, r.port),
		// 		ShouldResemble,
		// 		string(body))
		// })

		//////////TEST-TRIBE-ROUTES/////////////////

	})
}
