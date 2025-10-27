import configparser
import os
from typing import Optional, Dict, Any, Literal


class EnvInterpolation(configparser.BasicInterpolation):
    """
    Interpolation class that expands environment variables in config values.
    Example: `host = $HOST` will be replaced by the value of the 'HOST' env var.
    """

    def before_get(self, parser, section, option, value, defaults):
        value = os.path.expandvars(value)
        if "$" in value:
            return os.environ.get(value.replace("$", ""), value)
        return value


class EnvConfigParser(configparser.ConfigParser):
    """
    A ConfigParser that automatically uses the EnvInterpolation class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, interpolation=EnvInterpolation(), **kwargs)


class TinyStreamConfig:
    """
    A unified config object for all TinyStream components.

    It can be initialized from an INI file (with env var support)
    or a dictionary, and intelligently determines the operation
    mode ("single" vs "cluster").
    """

    controller_config: Dict[str, Any]
    broker_config: Dict[str, Any]
    serialization: Dict[str, Any]
    metastore: Dict[str, Any]

    mode: Literal["single", "cluster"]

    def __init__(
        self,
        controller_config: Optional[Dict[str, Any]] = None,
        broker_config: Optional[Dict[str, Any]] = None,
        serialization: Optional[Dict[str, Any]] = None,
        metastore: Optional[Dict[str, Any]] = None,
    ):
        """
        Initializes the config object.
        Use .from_ini() or .from_dict() factory methods instead.
        """
        self.controller_config = controller_config or {}
        self.broker_config = broker_config or {}
        self.serialization = serialization or {}
        self.metastore = metastore or {}

        if self.controller_config:
            self.mode = "cluster"
        elif self.broker_config:
            self.mode = "single"
        else:
            raise ValueError(
                "Config is empty. Must contain a [controller] or [broker] section."
            )

    @classmethod
    def from_ini(cls, file_path: str) -> "TinyStreamConfig":
        """
        Creates a TinyStreamConfig object from an INI file.

        This method now uses EnvConfigParser to support
        environment variable interpolation.
        """
        parser = EnvConfigParser()

        if not parser.read(file_path):
            raise FileNotFoundError(f"Config file not found or empty: {file_path}")

        config_dict = {section: dict(parser[section]) for section in parser.sections()}
        return cls.from_dict(config_dict)

    @classmethod
    def from_dict(cls, config: Dict[str, Any]):
        """Creates a TinyStreamConfig object from a dictionary."""
        controller_conf = config.get("controller")
        broker_conf = config.get("broker")
        serialization = config.get("serialization")
        metastore = config.get("metastore")

        return cls(
            controller_config=controller_conf,
            broker_config=broker_conf,
            serialization=serialization,
            metastore=metastore,
        )

    @classmethod
    def from_default_config_file(cls) -> "TinyStreamConfig":
        """Creates a TinyStreamConfig object from the default config file path."""
        default_path = os.getenv("TINYSTREAM_CONFIG_PATH", "tinystream.ini")
        return cls.from_ini(default_path)

    def get_controller_config(self) -> Dict[str, Any]:
        """Returns the [controller] section. Raises error if in single mode."""
        if self.mode == "single":
            raise ValueError("Cannot get controller config in 'single' mode.")
        return self.controller_config

    def get_serialization_config(self) -> Dict[str, Any]:
        """Returns the [serialization] section, or empty dict if not present."""
        return self.serialization

    def get_broker_config(self) -> Dict[str, Any]:
        """
        Returns the [broker] section.
        In cluster mode, this might be empty, which is fine.
        In single mode, this is the primary config.
        """
        return self.broker_config

    def get_metastore_config(self):
        """Returns the [metastore] section, or empty dict if not present."""
        return self.metastore
