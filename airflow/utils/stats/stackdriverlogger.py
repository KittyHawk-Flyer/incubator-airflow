from __future__ import print_function

from datetime import datetime, timedelta
import logging
from threading import Thread
import time
import warnings

from google.cloud.exceptions import BadRequest
from google.cloud.monitoring import MetricKind, ValueType
from google.cloud.monitoring.label import LabelDescriptor

from airflow.utils import process_type


class StackdriverLogger(object):
    def __init__(self, client, path_prefix):
        self.client = client
        self.path_prefix = path_prefix

        self.new_descs = {}
        self.registered_descs = {}
        self.counters = {}

    def start_publishing(self):
        self.publisher = Thread(
            None,
            StackdriverLogger._publish,
            'StackdriverLogger-publisher',
            (self.client, self.path_prefix, self.new_descs, self.registered_descs, self.counters)
        )
        self.publisher.daemon = True
        self.publisher.start()

    @staticmethod
    def _publish(client, path_prefix, new_descs, registered_descs, counters):
        cls = StackdriverLogger.__class__
        log = logging.root.getChild("airflow.StackdriverLogger")

        while True:
            next_wakeup = datetime.utcnow() + timedelta(minutes=1)
            try:
                StackdriverLogger._do_publish(
                    client,
                    path_prefix,
                    new_descs,
                    registered_descs,
                    counters,
                    log)
            except BadRequest as e:
                log.error("Failed to publish metrics", exc_info=e)

            time.sleep(max(0, (next_wakeup - datetime.utcnow()).total_seconds()))

    @staticmethod
    def _do_publish(
            client,
            path_prefix,
            new_descs,
            registered_descs,
            counters,
            log):
        log.info("Publishing...")
        # 1. register descriptors
        label = LabelDescriptor("process_type")
        for name, value_type in new_descs.items():
            # a "new" descriptoer might already exist in "registered" descriptor set because
            # registered_descs and new_descs are modified in a non-atomic way (see below)
            # filter our already-registered descriptors here.
            if name in registered_descs:
                continue

            desc = client.metric_descriptor(
                'custom.googleapis.com/%s/%s' % (path_prefix, name),
                metric_kind=MetricKind.GAUGE,
                value_type=value_type,
                labels=[label],
            )

            log.info("registering MetricDescriptor %s with type %s" % (desc.type, desc.value_type))
            desc.create()

            # Because the two operations below are not atomic. this may registere the same descriptor twice.
            # This is fine as we filter out dupes before creating descriptors. (see above)
            registered_descs[name] = desc
            del new_descs[name]

        # 2. write counters
        resource = client.resource('global', labels={})
        now = datetime.utcnow()
        ts = []

        for k, v in counters.items():
            desc = registered_descs.get(k)
            if desc:
                metric = client.metric(type_=desc.type, labels={
                    'process_type': process_type(),
                })
                ts.append(client.time_series(metric, resource, v, end_time=now))

        if len(ts) > 0:
            log.info("writing time series: %s" % [t.metric.type for t in ts])
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


