import json
from os import path, listdir

from termcolor import colored, cprint

from .utils import resource_path

class ConfigValidator:
    __config_dir = resource_path(r'configs')
    __default_structure = {
        'repo': {
            'remote_path': str,
            'local_path': {'env': (str, type(None)), 'path': str},
            'dist_path': {'env': (str, type(None)), 'path': str},
            'release_branch': str,
            'folder': str
        },
        'db': {
            'connection': {
                'host': str,
                'port': int,
                'dbname': str,
                'user': str
            },
            'log_table': str
        },
        'misc': {'deploy_mode': str}
    }

    def __init__(self):
        self._valid_configs = []
        self._invalid_configs = []
        self._selected_config = None

    @property
    def valid_configs(self):
        return '\n'.join(self._valid_configs)

    @property
    def invalid_configs(self):
        return '\n'.join(self._invalid_configs)

    def validate_config(self):
        if not path.exists(self.__config_dir):
            raise FileNotFoundError(f'Folder {self.__config_dir} not found.')

        for filename in sorted(listdir(self.__config_dir)):
            if filename.endswith('.json'):
                file_path = path.join(self.__config_dir, filename)
                if self._is_valid_config(file_path):
                    self._valid_configs.append(filename)
                else:
                    self._invalid_configs.append(filename)
        print(colored(f'Invalid config files:\n{self.invalid_configs}', 'red', attrs=['bold']))
        print(colored(f'Valid config files:\n{self.valid_configs}', 'light_green', attrs=['bold']))

        if self._valid_configs:
            self._selected_config = self._valid_configs[0]
            cprint(f'Default config file is: {self._selected_config}', color='cyan')
            return self._prompt_user_selection()
        raise FileNotFoundError('No valid configuration files found.')

    @staticmethod
    def load_config(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    def _is_valid_config(self, file_path):
        try:
            data = self.load_config(file_path)
            return self._validate_structure(data, self.__default_structure)
        except (json.JSONDecodeError, IOError):
            return False

    def _validate_structure(self, data, structure):
        if not isinstance(data, dict):
            return False
        for key, expected_type in structure.items():
            if key not in data:
                return False
            if isinstance(expected_type, dict):
                if not self._validate_structure(data[key], expected_type):
                    return False
            elif not isinstance(data[key], expected_type):
                return False
        return True

    def _prompt_user_selection(self):
        cprint('Enter the configuration file name (or press Enter to use the default): ', 'cyan')
        user_choice = input().strip()
        if user_choice and user_choice in self._valid_configs:
            self._selected_config = user_choice
            cprint(f'Selected file: {self._selected_config}', 'light_green')
        else:
            cprint(f'Using default file: {self._selected_config}', 'light_green')

        _config = self.load_config(fr'{self.__config_dir}\{self._selected_config}')
        return _config
