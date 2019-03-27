from datetime import datetime
from json import (
    dumps as json_dumps,
)
from requests import (
    get as requests_get,
    post as requests_post,
)

from vinchainio.vinchain import VinChain
from vinchain_hashing import hash_functions

from vinchain_database_hasher.conf import settings

import sys
import logging
import logstash

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)
_logger.addHandler(logstash.TCPLogstashHandler(settings.logstash_host, settings.logstash_port,
                                               message_type=settings.app_name, version=settings.logging_version))
_logger.addHandler(logging.StreamHandler(sys.stdout))


def get_vehicle_model():
    path, model_name = settings.vehicle_model
    return getattr(
        __import__(path, fromlist=[model_name]),
        model_name
    )


def get_vehicle_serializer():
    path, serializer = settings.vehicle_serializer
    return getattr(
        __import__(path, fromlist=[serializer]),
        serializer
    )


def get_last_sent_id():
    row = requests_get(
        '{}/vindb/vin_records/last/'.format(settings.vindb_host),
        params={
            'data_source': settings.vindb_data_source,
        },
        headers={
            'Content-Type': 'application/json'
        }
    ).json()

    return row.get('uuid', 0)


def get_new_rows(model, latest_hashed_id, qty_rows):
    new_rows = list(
        model.objects.values().filter(
            id__gt=latest_hashed_id
        ).order_by(settings.vehicle_model_primary_key)[:qty_rows]
    )
    return new_rows


def get_latest_id(model):
    return int(getattr(model.objects.latest(settings.vehicle_model_primary_key), settings.vehicle_model_primary_key, 0))


def dummy_serializer(row):
    return row


def hash_rows(stop_flag):
    latest_hashed = get_last_sent_id()

    have_new_rows = True
    hashed_rows = 0

    model = get_vehicle_model()
    serializer = get_vehicle_serializer()

    while not stop_flag[0] and have_new_rows:
        latest_id = get_latest_id(model)
        new_rows = get_new_rows(model, latest_hashed, settings.max_size_hashed_batch)
        records = []

        if not len(new_rows):
            have_new_rows = False

        for new_row in new_rows:
            if new_row['vin'] is None:
                continue

            latest_hashed = new_row[settings.vehicle_model_primary_key]
            hashed_rows += 1

            records.append({
                'uuid': new_row[settings.vehicle_model_primary_key],
                'vin': new_row[settings.vehicle_model_vin_key],
                'standard_version': settings.vindb_hash_functions,
                'hash': hash_functions[
                    settings.vindb_hash_functions
                ](serializer(new_row))
            })

        if len(records):
            blockchain = VinChain(
                node=settings.vinchain_node,
                blocking=True,
                debug=False,
                known_chains={
                    'VIN': {
                        'chain_id': settings.vinchain_chain_id,
                        'core_symbol': 'VIN',
                        'prefix': 'VIN'
                    },
                }
            )
            blockchain.wallet.unlock(settings.vinchain_wallet_password)

            payload = {
                'signature': blockchain.get_message(
                    datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                ).sign(
                    settings.vindb_hasher if settings.vindb_use_hasher else settings.vindb_data_source
                ),
                'data_source': settings.vindb_data_source,
                'hashes': records
            }

            if settings.vindb_use_hasher:
                payload['hasher'] = settings.vindb_hasher

            response = requests_post(
                '{}/vindb/vin_records/create/'.format(settings.vindb_host),
                data=json_dumps(payload),
                headers={
                    'Content-Type': 'application/json'
                }
            )

            extra = {
                'data_source': settings.vindb_data_source,
                'hash_functions': settings.vindb_hash_functions,
                'latest_hashed_id': latest_hashed,
                'latest_id': latest_id,
                'success': response.status_code == 201,
            }

            if response.status_code != 201:  # error
                extra['result'] = json_dumps({'status_code': response.status_code, 'response': response.text}),
                _logger.error('%s:  %d rows processed unsuccessfully (ids %s-%s). Status code: %s. Error: "%s"',
                              settings.app_name, len(records),
                              records[0]['uuid'], records[-1]['uuid'], response.status_code, response.text, extra=extra)
                raise Exception('Rows have not been stored in DB. Status code: {}. Error: "{}"'.format(
                    response.status_code, response.text)
                )

            # success
            hashed_records = response.json()['records']
            # check if all records stored in DB
            rs = len(hashed_records) == len(records)
            extra.update(
                {
                    'success': rs,
                    'hashed_rows': len(hashed_records),
                    'hashed_rows_ids': [r['uuid'] for r in hashed_records],
                    'tried_hash_rows_ids': [r['uuid'] for r in records] if not rs else None,
                    'result': json_dumps({'status_code': response.status_code}),
                }
            )
            if rs:
                _logger.info('%s: %d rows processed successfully (ids %s-%s)', settings.app_name, len(hashed_records),
                             hashed_records[0]['uuid'], hashed_records[-1]['uuid'], extra=extra)
            else:
                # _logger.error('%s: Not all rows have been stored in DB. '
                #              'Only %d from %d rows processed successfully (ids %s-%s)',
                #              settings.app_name, len(hashed_records), len(records),
                #              hashed_records[0]['uuid'], hashed_records[-1]['uuid'], extra=extra)
                raise Exception('Not all rows have been created. Status code: {}. Hashed rows ids: "{}". '
                                'Tried to hash rows ids: "{}"'.format(response.status_code, extra['hashed_rows_ids'],
                                                                      extra['tried_hash_rows_ids']))

    return hashed_rows
