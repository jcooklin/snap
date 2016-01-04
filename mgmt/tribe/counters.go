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

	log "github.com/Sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrUpdateCounter = errors.New("updating counter failed")
)

type counters struct {
	intentGauges  *prometheus.GaugeVec
	msgCounterVec *prometheus.CounterVec
	logger        *log.Entry
}

func newCounters() *counters {
	c := &counters{
		intentGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "snapd",
				Subsystem: "tribe",
				Name:      "intents",
				Help:      "The number of intents.",
			},
			[]string{"all"},
		),
		msgCounterVec: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "snapd",
				Subsystem: "tribe",
				Name:      "messages_received",
				Help:      "The number of messages received.",
			},
			[]string{"type"},
		),
		logger: logger.WithField("_module", "tribe-counters"),
	}
	prometheus.MustRegister(c.intentGauges)
	prometheus.MustRegister(c.msgCounterVec)
	return c
}

func (c *counters) incIntentCounter(mt msgType) {
	metric, err := c.intentGauges.GetMetricWith(prometheus.Labels{"type": "all"})
	if err != nil {
		c.logger.WithFields(
			log.Fields{
				"_block": "incIntentCounter",
				"err":    err,
			},
		).Error(ErrUpdateCounter)
		return
	}
	metric.Inc()
	metric, err = c.intentGauges.GetMetricWith(prometheus.Labels{"type": mt.String()})
	if err != nil {
		c.logger.WithFields(
			log.Fields{
				"_block": "incIntentCounter",
				"err":    err,
			},
		).Error(ErrUpdateCounter)
		return
	}
	metric.Inc()
}

func (c *counters) decIntentCounter(mt msgType) {
	metric, err := c.intentGauges.GetMetricWith(prometheus.Labels{"type": "all"})
	if err != nil {
		c.logger.WithFields(log.Fields{"_block": "removeIntent"}).Error(err)
		return
	}
	metric.Dec()
	metric, err = c.intentGauges.GetMetricWith(prometheus.Labels{"type": mt.String()})
	if err != nil {
		c.logger.WithFields(log.Fields{"_block": "removeIntent"}).Error(err)
		return
	}
	metric.Dec()
}

func (c *counters) incNotifyMsgCounters(mt msgType) {
	metric, err := c.msgCounterVec.GetMetricWith(prometheus.Labels{"type": "all"})
	if err != nil {
		c.logger.WithFields(
			log.Fields{
				"_block": "incNotifyMsgCounters",
				"err":    err,
			},
		).Error(ErrUpdateCounter)
		return
	}
	metric.Inc()
	metric, err = c.msgCounterVec.GetMetricWith(prometheus.Labels{"type": mt.String()})
	if err != nil {
		c.logger.WithFields(
			log.Fields{
				"_block": "incNotifyMsgCounters",
				"err":    err,
			},
		).Error(ErrUpdateCounter)
		return
	}
	metric.Inc()
}
