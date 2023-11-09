#
# Copyright 2020-2023 Ghent University
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
Main kafka based class for producers and consumer
"""

import logging
import json

from datetime import datetime, timedelta

from kafka import KafkaProducer, KafkaConsumer

from vsc.utils.timestamp import convert_to_datetime
from vsc.utils.script_tools import NrpeCLI


def make_time(ts, fmt="%Y-%m-%d", begin=False, end=False):
    """
    Return timestamp in format fmt

    If begin if True, round down till begin of day
    If end is True, round up till end of day
    """

    dt = convert_to_datetime(ts)
    if begin or end:
        dt = datetime(*dt.timetuple()[:3])
        if end:
            # add 1 day, subtract 1 second
            dt += timedelta(days=1)
            dt -= timedelta(seconds=1)

    return datetime.strftime(dt, fmt)



class KafkaCLI(NrpeCLI):
    """
    Base class for Kafka based NrpeCLI clients
    """
    KAFKA_COMMON_OPTIONS = {
        'topic': ("Kafka topics to produce/consume", None, "store", "xdmod"),
        'brokers': ("List of kafka brokers, comma separated", "strlist", "store", None),
        'security_protocol': ("Security protocol to use, e.g., SASL_SSL", str, "store", "PLAINTEXT"),
        'ssl': ("Comma-separated key=value list of SSL options for underlying kafka lib", "strlist", "store", []),
        'sasl': ("Comma-separated key=value list of SASL options for the underlying kafka lib", "strlist", "store", []),
        # Very advanced/dangerous usage:
        # e.g. on initial usage of consumer, pass --kafka=auto_offset_reset=earliest
        #    to start from the earliest offset in abscence of (first) commit
        'kafka': ("Comma-separated key=value list of allowed options for the underlying kafka lib",
                  "strlist", "store", []),
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.stats = {}



    def make_options(self, defaults=None):
        self.CLI_OPTIONS.update(self.KAFKA_COMMON_OPTIONS)
        return super().make_options(defaults=defaults)

    def get_kafka_kwargs(self):
        """Generate the kafka producer or consumer args"""
        kwargs = dict(map(lambda kv: kv.split('='), self.options.ssl + self.options.sasl + self.options.kafka))

        kwargs['bootstrap_servers'] = self.options.brokers
        kwargs['security_protocol'] = self.options.security_protocol

        return kwargs

    def make_consumer(self, group):
        """Return consumer instance for specific topic and group"""

        return KafkaConsumer(
            self.options.topic,
            group_id=group,
            **self.get_kafka_kwargs()
        )


class ProducerCLI(KafkaCLI):
    # Resource identifier
    PRODUCER_TYPE = None
    START_END_TIME_FORMAT = None

    KAFKA_COMMON_PRODUCER_OPTIONS = {
        'end_timestamp': ("End time for events (default now)", str, "store", None),
        'max_delta': ("Maximum number of days between start and end time", str, "store", 7),
    }

    def check_time(self):
        start, end = self._start_end_datetime()
        delta = end - start
        max_delta = timedelta(days=float(self.options.max_delta))
        logging.debug("check_time start %s end %s delta %s max_delta %s", start, end, delta, max_delta)
        if delta >= max_delta:
            logging.error("Delta %s between start %s and end %s is more than max_delta %s",
                          delta, start, end, max_delta)
            raise ValueError("Max start end timedelta exceeded")

    def _start_end_datetime(self):
        """return start and end datetime tuple"""
        # Pick up where we left off the last successful run
        # The saved timestamp is always on the day of the end date
        # which is "the next day" for the sacct run, i.e., sacct run
        # is up to and including the day before the producer runs
        end_timestamp = self.options.end_timestamp
        if end_timestamp is None:
            logging.info("Relying on current time for end_time: %s", self.current_time)
            end_timestamp = self.current_time

        # do not use self.options.start_timestamp, it is not updated with timestamp cache
        return convert_to_datetime(self.start_timestamp), convert_to_datetime(end_timestamp)

    def start_end_time(self):
        """Return formatted start and end time"""

        start, end = self._start_end_datetime()

        return (
            make_time(start, fmt=self.START_END_TIME_FORMAT, begin=True),
            make_time(end, fmt=self.START_END_TIME_FORMAT, end=True),
        )

    def make_options(self, defaults=None):
        self.CLI_OPTIONS.update(self.KAFKA_COMMON_PRODUCER_OPTIONS)
        return super().make_options(defaults=defaults)

    def make_day(self, event):
        """Return datetime instance associated with event"""
        raise NotImplementedError

    def produce_value(self, resource, event):
        """Pass event, return dict to produce"""
        return {
            'payload': event,
            'resource': resource,
            'type': self.PRODUCER_TYPE.value,
            'day': datetime.strftime(self.make_day(event), "%Y%m%d"),
        }

    def produce(self, resource, events, dry_run):
        """Produce the events of resource into kafka topic PRODUCER_TOPIC"""

        producer = KafkaProducer(
            acks='all',
            **self.get_kafka_kwargs()
        )

        logging.info("%s events for resource %s to send to topic %s", len(events), resource, self.options.topic)
        for event in events:
            value = self.produce_value(resource, event)
            if dry_run:
                logging.debug("Dry run: would send to topic %s: %s", self.options.topic, value)
            else:
                producer.send(topic=self.options.topic, value=json.dumps(value, sort_keys=True).encode('utf8'))

    def get_resource_events(self):
        """Return list of (resource, events) pairs"""
        raise NotImplementedError

    def do(self, dry_run):
        """Implement producer do"""
        self.check_time()

        for resource, events in self.get_resource_events():
            self.produce(resource, events, dry_run)


class ConsumerCLI(KafkaCLI):
    """Consume data from kafka topics and prepare it as xdmod shred file input"""

    CONSUMER_CLI_OPTIONS = {
        'group': ("Kafka consumer group", None, "store", "xdmod"),
        'timeout': ('Kafka consumer timeout in ms. If not set, loops forever', int, "store", None),
    }

    def make_options(self, defaults=None):
        self.CLI_OPTIONS.update(self.CONSUMER_CLI_OPTIONS)
        return super().make_options(defaults=defaults)


    def get_kafka_kwargs(self):
        """Generate the kafka producer or consumer args"""

        kwargs = super().get_kafka_kwargs()

        if self.options.timeout is not None:
            kwargs["consumer_timeout_ms"] = self.options.timeout

        # disable auto commit, so dry-run doesn't commit
        kwargs.setdefault('enable_auto_commit', not self.options.dry_run)  # no autocommit is we dry-run

        return kwargs

    def convert_msg(self, msg):
        """
        Process msg as JSON.
        Return None on failure.
        """
        value = msg.value
        if value:
            try:
                event = json.loads(value)
            except ValueError:
                logging.error("Failed to load as JSON: %s", value)
                return None

            if 'payload' in event:
                return event
            else:
                logging.error("Payload missing from event %s", event)
                return None
        else:
            logging.error("msg has no value %s (%s)", msg, type(msg))
            return None

    def process_event(self, event, dry_run):
        """
        To be implemented in subclasses
        """

    def do(self, dry_run):
        """Consume data from kafka"""
        consumer = self.make_consumer(self.options.group)


        def consumer_close():
            # default is autocommit=True, which is not ok wrt dry_run
            consumer.close(autocommit=not dry_run)

            total = sum([sum(d.values()) for r in self.stats.values() for d in r.values()])
            logging.info("All %s messages retrieved (dry_run=%s): %s", total, dry_run, self.stats)

        logging.debug("Starting to iterate over messages")
        # we do not expect this loop to end, i.e., we keep polling
        for msg in consumer:
            event = self.convert_msg(msg)

            if event is not None:
                try:
                    self.process_event(event, dry_run)
                    if not dry_run:
                        # this is essentially one past the post ack, but we already have that message as well
                        consumer.commit()
                except Exception:
                    logging.exception("Something went wrong while processing event %s", event)
                    consumer_close()
                    raise

        consumer_close()
