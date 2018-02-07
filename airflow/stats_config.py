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
import logging
import os
import sys

from airflow import configuration as conf
from airflow.exceptions import AirflowConfigException
from airflow.utils import process_type
from airflow.utils.module_loading import import_string


class DummyStatsLogger(object):
    @classmethod
    def start_publishing(cls):
        pass

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


def configure_stats():
    stats_backend = conf.get('scheduler', 'stats_backend')
    if stats_backend == 'statsd' or conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        class StatsClientAdaptor(StatsClient):
            def __init__(self, host, port, prefix):
                super(StatsClientAdaptor, self).__init__(host, port, prefix)

            def start_publishing(self):
                pass

        return StatsClientAdaptor(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))

    elif stats_backend == 'stackdriver':
        # StackdriverLogger doesn't support multiprocess model like gunicorn
        if process_type() != 'scheduler':
            return DummyStatsLogger

        from google.cloud import monitoring
        from airflow.utils.stats.stackdriverlogger import StackdriverLogger

        client = monitoring.Client(conf.get('scheduler', 'stackdriver_project'))
        path_prefix = conf.get('scheduler', 'stackdriver_prefix')
        return StackdriverLogger(client, path_prefix)

    else:
        return DummyStatsLogger
