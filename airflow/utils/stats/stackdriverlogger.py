from datetime import datetime, timedelta
from threading import Thread
import time

from google.cloud.monitoring import MetricKind, ValueType

class StackdriverLogger(object):
    def __init__(self, client, path_prefix, start_publishing=True):
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

        if start_publishing:
            self.publisher.start()

    @staticmethod
    def _publish(client, path_prefix, new_descs, registered_descs, counters):
        '''
        Do NOT use logging in this function as it would highly likely trigger a deadlock.
        https://bugs.python.org/issue6721
        '''
        while True:
            next_wakeup = datetime.utcnow() + timedelta(minutes=1)
            StackdriverLogger._do_publish(client, path_prefix, new_descs, registered_descs, counters)
            time.sleep(max(0, (next_wakeup - datetime.utcnow()).total_seconds()))

    @staticmethod
    def _do_publish(client, path_prefix, new_descs, registered_descs, counters):
        '''
        Do NOT use logging in this function as it would highly likely trigger a deadlock.
        https://bugs.python.org/issue6721
        '''
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

        if len(ts) > 0:
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
        # log.info("Stats.gauge(%s, %s)" % (stat, value))
        desc = self._descriptor(stat, value)
        self._update_gauge(desc, value)

    def _update_gauge(self, desc, value):
        self.counters[desc] = value


    def timing(self, stat, dt):
        # Not implemented
        pass


