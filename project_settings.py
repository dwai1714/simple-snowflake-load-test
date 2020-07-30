import os

from st_connectors.key_vault.keyvault_secrets import SettingsConnector

settings = SettingsConnector(dynaconf_file= os.environ["SETTINGS_FILE_FOR_DYNACONF"])
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root
def get_root_path():
    return ROOT_DIR


