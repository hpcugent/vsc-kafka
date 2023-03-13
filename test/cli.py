#
# Copyright 2021-2023 Ghent University
#
# This file is part of vsc-kafka,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-kafka
#
# All rights reserved.
#
"""
xdmod tests
"""
import logging
import mock
import sys
import json

from collections import namedtuple


logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

from vsc.kafka.cli import make_time, ConsumerCLI


from vsc.install.testing import TestCase

class TestSlurm(TestCase):

    def test_make_time(self):
        ts = '1933142400'  # TZ=UTC date -d 2031-04-05T08:00:00 +%s
        self.assertEqual(make_time(ts), '2031-04-05')
        self.assertEqual(make_time(ts, fmt='%Y-%m-%dT%H:%M:%S'), '2031-04-05T08:00:00')
        self.assertEqual(make_time(ts, fmt='%Y-%m-%dT%H:%M:%S', begin=True), '2031-04-05T00:00:00')
        self.assertEqual(make_time(ts, fmt='%Y-%m-%dT%H:%M:%S', end=True), '2031-04-05T23:59:59')


class TestConsumer(TestCase):
    def setUp(self):
        """Prepare test case."""
        super(TestConsumer, self).setUp()

        sys.argv = [
            'name', '--debug',
            '--brokers=serv1,serv2',
            '--ssl=ssl1=sslv1,ssl2=sslv2',
            '--sasl=sasl1=saslv1,sasl2=sslv2',
            ]
    def mk_event(self, typ, resource, day, payload):
        return {
            "type": typ,
            "resource": resource,
            "payload": payload,
            "day": day,
        }


    @mock.patch('vsc.utils.script_tools.ExtendedSimpleOption.prologue')
    @mock.patch('vsc.kafka.cli.KafkaConsumer', autospec=True)
    def test_consume(self, mock_consumer, mock_prologue):

        cl1_1 = "test1_1|message1_1|cluster1|4|5|6|7|8|9|10|11|12|2020-02-28T01:02:03|14|15|16|17|18|19|20|21|22|23|24|25|26"
        cl1_2 = "test1_2|message1_1|cluster1|4|5|6|7|8|9|10|11|12|2020-02-28T11:02:03|14|15|16|17|18|19|20|21|22|23|24|25|26"
        cl2_1 = "test2_1|message2_1|cluster2|4|5|6|7|8|9|10|11|12|2020-02-29T04:05:06|14|15|16|17|18|19|20|21|22|23|24|25|26"

        def mk_msg(typ, resource, day, payload):
            return KafkaMsg(value=json.dumps(self.mk_event(typ, resource, day, payload), sort_keys=True).encode('utf8'))

        KafkaMsg = namedtuple("KafkaMsg", ["value"])

        args = [
            ('slurm', 'cluster1', '20200228', cl1_1),
            ('slurm', 'cluster1', '20200228', cl1_2),
            ('slurm', 'cluster2', '20200229', cl2_1),
        ]

        msgs = [mk_msg(*arg) for arg in args]

        m_iter = mock.MagicMock()
        m_iter.__iter__.return_value = msgs
        mock_consumer.return_value = m_iter

        consumer = ConsumerCLI()
        consumer.do(dry_run=False)

        logging.debug("consumer calls: %s", mock_consumer.mock_calls)
        self.assertEqual(len(mock_consumer.mock_calls), 2 + len(msgs) + 1)

        # init of consumer
        name, args, kwargs = mock_consumer.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, ('xdmod',))  # topic
        self.assertEqual(kwargs, {
            'group_id': 'xdmod',
            'security_protocol': 'PLAINTEXT',
            'bootstrap_servers': ['serv1', 'serv2'],
            'ssl1': 'sslv1',
            'ssl2': 'sslv2',
            'sasl1': 'saslv1',
            'sasl2': 'sslv2',
            'enable_auto_commit': True,
        })

        # next call is the __iter__ call for the loop
        name, args, kwargs = mock_consumer.mock_calls[1]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '().__iter__')
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

        # next are the message commits
        for idx in range(len(msgs)):
            name, args, kwargs = mock_consumer.mock_calls[2+idx]
            logging.debug("%s %s %s", name, args, kwargs)
            self.assertEqual(name, '().commit')
            self.assertEqual(args, ())
            self.assertEqual(kwargs, {})

        # last call is the close call
        name, args, kwargs = mock_consumer.mock_calls[-1]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '().close')
        self.assertEqual(args, ())
        self.assertEqual(kwargs, {'autocommit': True})
