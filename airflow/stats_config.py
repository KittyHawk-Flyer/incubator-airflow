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


log = logging.getLogger(__name__)


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


def prepare_classpath():
    config_path = os.path.join(conf.get('core', 'airflow_home'), 'config')
    config_path = os.path.expanduser(config_path)

    if config_path not in sys.path:
        sys.path.append(config_path)


def configure_stats():
    # Prepare the classpath so we are sure that the config folder
    # is on the python classpath and it is reachable
    prepare_classpath()

    stats_class_path = None
    try:
        stats_class_path = conf.get('core', 'stats_class')
    except AirflowConfigException:
        log.debug('Could not find key logging_config_class in config')

    if stats_class_path:
        try:
            stats_instance = import_string(stats_class_path)

            log.info(
                'Successfully instantiated user-defined stats class from %s',
                stats_class_path
            )

            return stats_instance
        except Exception as e:
            msg = 'Unable to load custom stats from {}'.format(stats_class_path)
            log.error(msg, exc_info=e)
            raise ImportError(msg)

    if conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        return StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))

    else:
        return DummyStatsLogger
