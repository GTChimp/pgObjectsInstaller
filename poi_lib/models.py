from os import path, getenv
from dataclasses import dataclass
from enum import Enum


class DeployType(Enum):
    RELEASE = 'release'
    REVERT = 'revert'


class RevertStage(Enum):
    ZERO = 0
    ONE = 1
    TWO = 2


class DeployMode(Enum):
    SEPARATE_STATEMENTS = 'separate'
    SINGLE_STATEMENT = 'single'


@dataclass
class RepositoryConfig:
    remote_path: str
    local_path: str
    dist_path: str
    release_branch: str
    folder: str
    revert_branch: str = None

    def __init__(self, properties_dict):
        for k, v in properties_dict.items():
            self.__setattr__(k, v)

    def __repr__(self):
        t = ', '.join(f'{k}={v}' for k, v in self.__dict__.items())
        return f'{self.__class__.__name__}({t})'

    def __setattr__(self, key, value):
        def decode_init_local_path(local_path: dict):
            if local_path['env'] is None:
                return path.abspath(local_path['path'])
            return path.abspath(getenv(local_path['env']) + local_path['path'])

        if isinstance(value, dict):
            value = decode_init_local_path(value)
        if (key in self.__dict__ and value != '') or key not in self.__dict__:
            object.__setattr__(self, key, value)


class PGConnectionConfig:
    def __init__(self, properties_dict):
        for k, v in properties_dict.items():
            object.__setattr__(self, k, v)

    def __repr__(self):
        t = ', '.join(f'{k}={v}' for k, v in self.__dict__.items())
        return f'{self.__class__.__name__}({t})'

    def __setattr__(self, key, value):
        if (key in self.__dict__ and value != '') or key not in self.__dict__:
            object.__setattr__(self, key, value)

    def as_dict(self):
        return {k: v for k, v in self.__dict__.items() if not callable(v)}
