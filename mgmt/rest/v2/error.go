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

package v2

import (
	"fmt"

	"errors"

	"github.com/intelsdi-x/snap/core/serror"
)

// Error string constants
const (
	ErrPluginAlreadyLoaded     = "plugin is already loaded"
	ErrTaskNotFound            = "task not found"
	ErrTaskDisabledNotRunnable = "task is disabled"
)

// Errors
var (
	ErrPluginNotFound       = errors.New("plugin not found")
	ErrStreamingUnsupported = errors.New("streaming unsupported")
	ErrNoActionSpecified    = errors.New("no action was specified in the request")
	ErrWrongAction          = errors.New("wrong action requested")
	ErrReadRequestBody      = errors.New("Read request body error")
)

// Error
//
// swagger:response ErrorResponse
type ErrorResp struct {
	// in:body
	SnapError Error `json: "snap_error""`
}

type Error struct {
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields"`
}

func FromSnapError(pe serror.SnapError) *Error {
	e := &Error{Message: pe.Error(), Fields: make(map[string]string)}
	// Convert into string format
	for k, v := range pe.Fields() {
		e.Fields[k] = fmt.Sprint(v)
	}
	return e
}

func FromSnapErrors(errs []serror.SnapError) *Error {
	fields := make(map[string]string)
	var msg string
	for i, err := range errs {
		for k, v := range err.Fields() {
			fields[fmt.Sprintf("%s_err_%d", k, i)] = fmt.Sprint(v)
		}
		msg = msg + fmt.Sprintf("error %d: %s ", i, err.Error())
	}
	return &Error{
		Message: msg,
		Fields:  fields,
	}
}

func FromError(err error) *Error {
	e := &Error{Message: err.Error(), Fields: make(map[string]string)}
	return e
}
