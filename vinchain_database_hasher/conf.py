# -*- coding: utf-8 -*-

from django.conf import settings as app_setting
from django.test.signals import setting_changed


DEFAULTS = {
    'app_name': 'vinchain_database_hasher',
    'vehicle_model': ('path', 'model'),
    'vehicle_model_primary_key': 'id',
    'vehicle_model_vin_key': 'vin',
    'vehicle_serializer': ('vinchain_database_hasher.tasks', 'dummy_serializer'),
    'vinchain_node': '',
    'vinchain_wallet_password': '',
    'vinchain_chain_id': '',
    'vindb_data_source': '',
    'vindb_hash_functions': 0,
    'vindb_hasher': '',
    'vindb_host': '',
    'vindb_use_hasher': False,
    'max_size_hashed_batch': 0,
    'logstash_host': 'localhost',
    'logstash_port': 5100,
    'logging_version': 1,
}


class VinchainDatabaseHasherSettings(object):
    def __init__(self, user_settings=None, defaults=None):
        self.__user_settings = user_settings or {}
        self.__defaults = defaults or DEFAULTS

    def __getattr__(self, attr):
        assert not attr.startswith('__')

        try:
            val = self.__user_settings[attr]
        except KeyError:
            val = self.__defaults[attr]

        setattr(self, attr, val)

        return val


settings = VinchainDatabaseHasherSettings(
    user_settings=getattr(app_setting, 'VINCHAIN_DATABASE_HASHER', {}),
    defaults=DEFAULTS
)


def reload_settings(*args, **kwargs):
    global settings
    setting, value = kwargs['setting'], kwargs['value']
    if setting == 'VINCHAIN_DATABASE_HASHER':
        settings = VinchainDatabaseHasherSettings(
            user_settings=value,
            defaults=DEFAULTS
        )


setting_changed.connect(reload_settings)
