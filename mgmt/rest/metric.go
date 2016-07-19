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
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/mgmt/rest/rbody"
)

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	ver := -1 // -1: get latest version

	// If we are provided a parameter with the name 'ns' we need to
	// perform a query
	q := r.URL.Query()
	v := q.Get("ver")
	ns_query := q.Get("ns")
	if ns_query != "" {
		ver = -1 // -1: get latest version
		if v != "" {
			var err error
			ver, err = strconv.Atoi(v)
			if err != nil {
				respond(400, rbody.FromError(err), w)
				return
			}
		}
		// strip the leading '/' and split on the remaining '/'
		ns := strings.Split(strings.TrimLeft(ns_query, "/"), "/")
		if ns[len(ns)-1] == "*" {
			ns = ns[:len(ns)-1]
		}

		mets, err := s.mm.FetchMetrics(core.NewNamespace(ns...), ver)
		if err != nil {
			respond(404, rbody.FromError(err), w)
			return
		}
		respondWithMetrics(r.Host, mets, w)
		return
	}

	mets, err := s.mm.MetricCatalog()
	if err != nil {
		respond(500, rbody.FromError(err), w)
		return
	}
	respondWithMetrics(r.Host, mets, w)
}

func (s *Server) getMetricsFromTree(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	namespace := params.ByName("namespace")

	// we land here if the request contains a trailing slash, because it matches the tree
	// lookup URL: /v1/metrics/*namespace.  If the length of the namespace param is 1, we
	// redirect the request to getMetrics.  This results in GET /v1/metrics and
	// GET /v1/metrics/ behaving the same way.
	if len(namespace) <= 1 {
		s.getMetrics(w, r, params)
		return
	}

	ns := parseNamespace(namespace)

	var (
		ver int
		err error
	)
	q := r.URL.Query()
	v := q.Get("ver")

	if ns[len(ns)-1] == "*" {
		if v == "" {
			ver = -1 //return latest metrics
		} else {
			ver, err = strconv.Atoi(v)
			if err != nil {
				respond(400, rbody.FromError(err), w)
				return
			}
		}

		mets, err := s.mm.FetchMetrics(core.NewNamespace(ns[:len(ns)-1]...), ver)
		if err != nil {
			respond(404, rbody.FromError(err), w)
			return
		}
		respondWithMetrics(r.Host, mets, w)
		return
	}

	// If no version was given, get all that fall at this namespace.
	if v == "" {
		mts, err := s.mm.GetMetricVersions(core.NewNamespace(ns...))
		if err != nil {
			respond(404, rbody.FromError(err), w)
			return
		}
		respondWithMetrics(r.Host, mts, w)
		return
	}

	// if an explicit version is given, get that single one.
	ver, err = strconv.Atoi(v)
	if err != nil {
		respond(400, rbody.FromError(err), w)
		return
	}
	mt, err := s.mm.GetMetric(core.NewNamespace(ns...), ver)
	if err != nil {
		respond(404, rbody.FromError(err), w)
		return
	}

	b := &rbody.MetricReturned{}
	dyn, indexes := mt.Namespace().IsDynamic()
	var dynamicElements []rbody.DynamicElement
	if dyn {
		dynamicElements = getDynamicElements(mt.Namespace(), indexes)
	}
	mb := &rbody.Metric{
		Namespace:       mt.Namespace().String(),
		Version:         mt.Version(),
		Dynamic:         dyn,
		DynamicElements: dynamicElements,
		Description:     mt.Description(),
		Unit:            mt.Unit(),
		LastAdvertisedTimestamp: mt.LastAdvertisedTime().Unix(),
		Href: catalogedMetricURI(r.Host, mt),
	}
	rt := mt.Policy().RulesAsTable()
	policies := make([]rbody.PolicyTable, 0, len(rt))
	for _, r := range rt {
		policies = append(policies, rbody.PolicyTable{
			Name:     r.Name,
			Type:     r.Type,
			Default:  r.Default,
			Required: r.Required,
			Minimum:  r.Minimum,
			Maximum:  r.Maximum,
		})
	}
	mb.Policy = policies
	b.Metric = mb
	respond(200, b, w)
}

func respondWithMetrics(host string, mets []core.CatalogedMetric, w http.ResponseWriter) {
	b := rbody.NewMetricsReturned()

	for _, met := range mets {
		rt := met.Policy().RulesAsTable()
		policies := make([]rbody.PolicyTable, 0, len(rt))
		for _, r := range rt {
			policies = append(policies, rbody.PolicyTable{
				Name:     r.Name,
				Type:     r.Type,
				Default:  r.Default,
				Required: r.Required,
				Minimum:  r.Minimum,
				Maximum:  r.Maximum,
			})
		}
		dyn, indexes := met.Namespace().IsDynamic()
		var dynamicElements []rbody.DynamicElement
		if dyn {
			dynamicElements = getDynamicElements(met.Namespace(), indexes)
		}
		b = append(b, rbody.Metric{
			Namespace:               met.Namespace().String(),
			Version:                 met.Version(),
			LastAdvertisedTimestamp: met.LastAdvertisedTime().Unix(),
			Description:             met.Description(),
			Dynamic:                 dyn,
			DynamicElements:         dynamicElements,
			Unit:                    met.Unit(),
			Policy:                  policies,
			Href:                    catalogedMetricURI(host, met),
		})
	}
	sort.Sort(b)
	respond(200, b, w)
}

func catalogedMetricURI(host string, mt core.CatalogedMetric) string {
	return fmt.Sprintf("%s://%s/v1/metrics?ns=%s&ver=%d", protocolPrefix, host, mt.Namespace().String(), mt.Version())
}

func getDynamicElements(ns core.Namespace, indexes []int) []rbody.DynamicElement {
	elements := make([]rbody.DynamicElement, 0, len(indexes))
	for _, v := range indexes {
		e := ns.Element(v)
		elements = append(elements, rbody.DynamicElement{
			Index:       v,
			Name:        e.Name,
			Description: e.Description,
		})
	}
	return elements
}
