from datetime import datetime
from signal import signal, SIGINT, SIGTERM
from time import sleep

from django.core.management.base import BaseCommand

from vinchain_database_hasher.tasks import hash_rows
from vinchain_database_hasher.conf import settings

import sys
import logging
import logstash

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)
_logger.addHandler(logstash.TCPLogstashHandler(settings.logstash_host, settings.logstash_port,
                                               message_type=settings.app_name, version=settings.logging_version))
_logger.addHandler(logging.StreamHandler(sys.stdout))

_logger_extra = {
    'data_source': settings.vindb_data_source,
}

class Command(BaseCommand):
    help = 'Send vehicle hashes to vindb'
    stop = [False]

    def __init__(self, *args, **kwargs):
        signal(SIGINT, self.stop_gracefully)
        signal(SIGTERM, self.stop_gracefully)

        super().__init__(*args, **kwargs)

    def stop_gracefully(self, signum, frame):
        _logger.warning('%s: Trying to stop', settings.app_name, extra=_logger_extra)
        self.stop[0] = True

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval', type=int, help='The interval with which new records will be checked', default=300
        )

        super(Command, self).add_arguments(parser)

    def handle(self, *app_labels, **options):
        _logger.warning('%s: Hashing started', settings.app_name, extra=_logger_extra)

        try:
            interval = 0
            while not self.stop[0]:
                if interval == 0:
                    hashed = hash_rows(self.stop)
                    interval += 1
                    self.stdout.write('{}:  Hashed {} records'.format(
                        datetime.now().strftime('%Y-%m-%dT%H:%M:%S%Z'), hashed)
                    )
                elif interval == options['interval']:
                    interval = 0
                else:
                    # Stop gracefully ticker
                    interval += 1

                sleep(1)
        except Exception as e:
            _logger.exception('%s: Exception. Application has stopped!!!', settings.app_name, extra=_logger_extra)
            raise e

        _logger.warning('%s: Hashing stopped', settings.app_name, extra=_logger_extra)
