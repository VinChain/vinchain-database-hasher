from datetime import datetime
from signal import signal, SIGINT, SIGTERM
from time import sleep

from django.core.management.base import BaseCommand

from vinchain_database_hasher.tasks import hash_rows


class Command(BaseCommand):
    help = 'Send vehicle hashes to vindb'
    stop = False

    def __init__(self, *args, **kwargs):
        signal(SIGINT, self.stop_gracefully)
        signal(SIGTERM, self.stop_gracefully)

        super().__init__(*args, **kwargs)

    def stop_gracefully(self, signum, frame):
        self.stop = True

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval', type=int, help='The interval with which new records will be checked', default=300
        )

        super(Command, self).add_arguments(parser)

    def handle(self, *app_labels, **options):
        self.stdout.write('{}: Hashing started'.format(datetime.now().strftime('%Y-%m-%dT%H:%M:%S%Z')))

        interval = 0
        while not self.stop:
            if interval == 0:
                hashed = hash_rows()
                interval += 1
                self.stdout.write(
                    '{}: Hashed {} records'.format(datetime.now().strftime('%Y-%m-%dT%H:%M:%S%Z'), hashed)
                )
            elif interval == options['interval']:
                interval = 0
            else:
                # Stop gracefully ticker
                interval += 1

            sleep(1)

        self.stdout.write('{}: Hashing stopped'.format(datetime.now().strftime('%Y-%m-%dT%H:%M:%S%Z')))
