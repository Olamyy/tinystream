import os

DEFAULT_CONTROLLER_CONFIG_PATH = os.environ.get(
    "TINYSTREAM_DEFAULT_CONTROLLER_CONFIG_FILE", "tinystream/config/controller.ini"
)

DEFAULT_BROKER_CONFIG_PATH = os.environ.get(
    "TINYSTREAM_DEFAULT_BROKER_CONFIG_FILE", "tinystream/config/broker.ini"
)
