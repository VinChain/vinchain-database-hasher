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


def dummy_serializer(row):
    return row


def hash_rows():
    latest_hashed = get_last_sent_id()

    have_new_rows = True
    hashed_rows = 0

    model = get_vehicle_model()
    serializer = get_vehicle_serializer()

    while have_new_rows:
        new_rows = list(
            model.objects.values().filter(
                id__gt=latest_hashed
            ).order_by(settings.vehicle_model_primary_key)[:1000]
        )
        records = []

        if not len(new_rows):
            have_new_rows = False

        for new_row in new_rows:
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

            requests_post(
                '{}/vindb/vin_records/create/'.format(settings.vindb_host),
                data=json_dumps(payload),
                headers={
                    'Content-Type': 'application/json'
                }
            )

    return hashed_rows
