import unittest
from mock import patch, mock, MagicMock

from google.cloud import monitoring
from google.cloud.monitoring import MetricKind, ValueType

from airflow.utils.stats.stackdriverlogger import StackdriverLogger


class TestStackdriverLogger(unittest.TestCase):
    @patch('google.cloud.monitoring.Client')
    def test_StackdriverLogger(self, mock_client):
        prefix = 'pre'

        logger = StackdriverLogger(mock_client, prefix, start_publishing=False)
        logger.incr("ctr", 5)
        logger.decr("ctr", 2)
        logger.gauge("gauge", 4.2)
        logger.gauge("gauge", 10.10)

        self.assertEquals(3, logger.counters['ctr'])
        self.assertEquals(10.10, logger.counters['gauge'])
        self.assertEquals({
            'ctr': ValueType.INT64,
            'gauge': ValueType.DOUBLE,
        }, logger.new_descs)

        desc = MagicMock()
        mock_client.metric_descriptor.return_value = desc

        metric = MagicMock()
        mock_client.metric.return_value = metric

        StackdriverLogger._do_publish(
            mock_client,
            prefix,
            logger.new_descs,
            logger.registered_descs,
            logger.counters)

        mock_client.metric_descriptor.assert_called()
        args_metric_descriptor = mock_client.metric_descriptor.call_args_list
        self.assertEquals(2, len(args_metric_descriptor))
        for call in args_metric_descriptor:
            (args, kwargs) = call
            self.assertTrue(args[0].startswith('custom.googleapis.com/%s/' % prefix))

        desc.create.assert_called()
        self.assertEquals(0, len(logger.new_descs))

        mock_client.metric.assert_called()
        mock_client.time_series.assert_called()
        args_time_series = mock_client.time_series.call_args_list
        self.assertEquals(2, len(args_time_series))

        mock_client.write_time_series.assert_called()
        ts = mock_client.write_time_series.call_args
        self.assertEquals(2, len(ts))


if __name__ == '__main__':
    unittest.main()
