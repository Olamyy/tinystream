import configparser
import os


class EnvInterpolation(configparser.BasicInterpolation):
    def before_get(self, parser, section, option, value, defaults):
        return os.path.expandvars(value)


class EnvConfigParser(configparser.ConfigParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, interpolation=EnvInterpolation(), **kwargs)


def load_config(file_path: str) -> EnvConfigParser:
    config = EnvConfigParser()
    config.read(file_path)
    return config
