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
Consumer class
 - consume kafka topic
 - write files to be shredded
"""

import json
import logging
import os

from vsc.kafka.cli import KafkaCLI

class ConsumerCLI(KafkaCLI):
    """Consume data from kafka topics and prepare it as xdmod shred file input"""

    CONSUMER_CLI_OPTIONS = {
        'group': ("Kafka consumer group", None, "store", "xdmod"),
        'timeout': ('Kafka consumer timeout in ms. If not set, loops forever', int, "store", None),
    }

    def make_options(self, defaults=None):
        self.CLI_OPTIONS.update(self.CONSUMER_CLI_OPTIONS)
        return super(ConsumerCLI, self).make_options(defaults=defaults)


    def get_kafka_kwargs(self):
        """Generate the kafka producer or consumer args"""

        kwargs = super(ConsumerCLI, self).get_kafka_kwargs()

        if self.options.timeout is not None:
            kwargs["consumer_timeout_ms"] = self.options.timeout

        # disable auto commit, so dry-run doesn't commit
        kwargs.setdefault('enable_auto_commit', False)

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

    def process_event(self, event):
        """
        To be implemented in subclasses
        """
        pass

    def do(self, dry_run):
        """Consume data from kafka"""
        consumer = self.make_consumer(self.options.group)


        def consumer_close():
            # default is autocommit=True, which is not ok wrt dry_run
            consumer.close(autocommit=False)

            total = sum([sum(d.values()) for r in stats.values() for d in r.values()])
            logging.info("All %s messages retrieved (dry_run=%s): %s", total, dry_run, stats)

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
