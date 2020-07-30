import logging
import os
from datetime import datetime

from dynaconf import settings


class SettingsConnector:
    def __init__(self, dynaconf_file=None):
        try:
            self.dynaconf_file = dynaconf_file
            if self.dynaconf_file is not None:
                settings.load_file(path=dynaconf_file)
                if "ENV_FOR_DYNACONF" in os.environ:
                    settings.setenv(os.environ["ENV_FOR_DYNACONF"])
                else:
                    settings.setenv("development")
        except Exception as error:
            logging.error(
                f"error in SecretsConnector __init__() "
                f"error = {error} at {datetime.now()}"
            )
            raise

    def get_value(self, key_name):
        try:
            logging.info("key_name is " + key_name)
            value_of_key = settings.get(key_name)
            if value_of_key is not None:
                return value_of_key
            else:
                return os.environ[key_name]
            # elif self.client is not None:
            #     if '_' in key_name:
            #         key_name = key_name.replace('_', '-')
            #     value_of_key = self.client.get_parameter(Name=key_name, WithDecryption=True)['Parameter']
            #     return value_of_key['Value']

        except Exception:
            return None

    def __getitem__(self, key_name):
        return self.get_value(key_name)

    def get(self, key_name):
        return self.get_value(key_name)

    def __getattr__(self, key_name):
        return self.get_value(key_name)
