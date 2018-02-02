# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime, timedelta
import logging
import os
import sys
import time
from threading import Thread

from airflow import configuration as conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)


def prepare_classpath():
    config_path = os.path.join(conf.get('core', 'airflow_home'), 'config')
    config_path = os.path.expanduser(config_path)

    if config_path not in sys.path:
        sys.path.append(config_path)


class DummyStatsLogger(object):
    @classmethod
    def incr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def decr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False):
        pass

    @classmethod
    def timing(cls, stat, dt):
        pass


from google.cloud.monitoring import MetricKind, ValueType

class StackdriverLogger(object):
    def __init__(self, client, path_prefix, publish=True):
        self.new_descs = {}
        self.registered_descs = {}
        self.counters = {}

        self.publisher = Thread(
            None,
            StackdriverLogger._publish,
            'StackdriverLogger-publisher',
            (client, path_prefix, self.new_descs, self.registered_descs, self.counters)
        )
        self.publisher.daemon = True

        if publish:
            self.publisher.start()

    @staticmethod
    def _publish(client, path_prefix, new_descs, registered_descs, counters):
        while True:
            next_wakeup = datetime.utcnow() + timedelta(minutes=1)
            StackdriverLogger._do_publish(client, path_prefix, new_descs, registered_descs, counters)
            time.sleep(max(0, (next_wakeup - datetime.utcnow()).total_seconds()))

    @staticmethod
    def _do_publish(client, path_prefix, new_descs, registered_descs, counters):
        # 1. registere descriptors
        descs = new_descs.items()
        for name, value_type in descs:
            desc = client.metric_descriptor(
                'custom.googleapis.com/%s/%s' % (path_prefix, name),
                metric_kind=MetricKind.GAUGE,
                value_type=value_type,
            )

            desc.create()

            # Because the two operations below are not atomic. this may registere the same descriptor twice.
            # This is fine as long as the descriptor's parameters are the same
            registered_descs[name] = desc
            del new_descs[name]

        # 2. write counters
        resource = client.resource('global', labels={})
        now = datetime.utcnow()
        ts = []

        for k, v in counters.items():
            desc = registered_descs.get(k)
            if desc:
                metric = client.metric(type_=desc.type, labels={})
                ts.append(client.time_series(metric, resource, v, end_time=now))

        client.write_time_series(ts)


    def _value_type(self, value):
        if type(value) == int:
            return ValueType.INT64
        elif type(value) == float:
            return ValueType.DOUBLE
        else:
            return ValueType.VALUE_TYPE_UNSPECIFIED

    def _descriptor(self, name, value):
        if name not in self.new_descs and name not in self.registered_descs:
            vt = self._value_type(value)
            self.new_descs[name] = vt

        # the callers should treat this as a opaque value,
        # should not assume it's the same string as the input.
        return name

    def incr(self, stat, count=1, rate=1):
        desc = self._descriptor(stat, count)
        self._update_counter(desc, count)

    def decr(self, stat, count=1, rate=1):
        desc = self._descriptor(stat, count)
        self._update_counter(desc, -count)

    def _update_counter(self, desc, value):
        if desc not in self.counters:
            self.counters[desc] = value
        else:
            self.counters[desc] += value

    def gauge(self, stat, value, rate=1, delta=False):
        desc = self._descriptor(stat, value)
        self._update_gauge(desc, value)

    def _update_gauge(self, desc, value):
        self.counters[desc] = value


    def timing(self, stat, dt):
        # Not implemented
        pass


def configure_stats():
    stats_backend = conf.get('scheduler', 'stats_backend')
    if stats_backend == 'statsd' or conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        return StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))
    elif stats_backend == 'stackdriver':
        from google.cloud import monitoring

        client = monitoring.Client(conf.get('scheduler', 'stackdriver_project'))
        path_prefix = conf.get('scheduler', 'stackdriver_path_prefix')
        return StackdriverLogger(client, path_prefix)
    else:
        return DummyStatsLogger
