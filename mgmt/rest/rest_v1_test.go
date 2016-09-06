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
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	log "github.com/Sirupsen/logrus"
	"github.com/intelsdi-x/snap/mgmt/rest/fixtures"
)

var (
	LOG_LEVEL = log.WarnLevel
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
	//TODO bind mock task manager r.BindTaskManager(s)
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
		Convey("Get plugins - v1/plugins", func() {
			resp, err := http.Get(
				fmt.Sprintf("http://localhost:%d/v1/plugins", r.port))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, 200)
			body, err := ioutil.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			fmt.Print(string(body))
			So(
				fmt.Sprintf(fixtures.GET_PLUGINS_RESPONSE, r.port, r.port,
					r.port, r.port, r.port, r.port),
				ShouldResemble,
				string(body))
		})

	})
}
