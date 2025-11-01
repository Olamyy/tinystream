import argparse
from pathlib import Path
from typing import Dict, Any, Tuple, Literal

from tinystream import DEFAULT_CONTROLLER_CONFIG_PATH, DEFAULT_BROKER_CONFIG_PATH
from tinystream.config.parser import EnvConfigParser


def split_host_port(uri: str, default_host: str, default_port: int) -> Tuple[str, int]:
    try:
        host, port_str = uri.split(":")
        return host, int(port_str)
    except (ValueError, TypeError, AttributeError):
        return default_host, default_port


class ConfigManager:
    controller_config: Dict[str, Any]
    broker_config: Dict[str, Any]
    serialization: Dict[str, Any]
    metastore: Dict[str, Any]

    def __init__(
        self, args: argparse.Namespace, component_type: Literal["controller", "broker"]
    ):
        """
        Loads and resolves configuration.
        """

        parser = EnvConfigParser()

        if Path(DEFAULT_CONTROLLER_CONFIG_PATH).is_file():
            print(
                f"[ConfigManager] Loading default controller config: {DEFAULT_CONTROLLER_CONFIG_PATH}"
            )
            parser.read(DEFAULT_CONTROLLER_CONFIG_PATH)
        else:
            raise FileNotFoundError(
                f"Default controller config not found: {DEFAULT_CONTROLLER_CONFIG_PATH}"
            )

        if component_type == "broker":
            if Path(DEFAULT_BROKER_CONFIG_PATH).is_file():
                print(
                    f"[ConfigManager] Loading default broker config: {DEFAULT_BROKER_CONFIG_PATH}"
                )
                parser.read(DEFAULT_BROKER_CONFIG_PATH)
            else:
                raise FileNotFoundError(
                    f"Default broker config not found: {DEFAULT_BROKER_CONFIG_PATH}"
                )

        user_config_path = getattr(args, "config", None)
        if user_config_path:
            if Path(user_config_path).is_file():  # type: ignore
                print(f"[ConfigManager] Loading user config: {user_config_path}")
                parser.read(user_config_path)  # type: ignore
            else:
                raise FileNotFoundError(
                    f"User-specified config file not found: {user_config_path}"
                )

        self.controller_config = dict(parser.items("controller"))
        self.metastore = dict(parser.items("metastore"))
        self.serialization = dict(parser.items("serialization"))

        if component_type == "broker":
            self.broker_config = dict(parser.items("broker"))

        if getattr(args, "controller_uri", None):
            host, port = split_host_port(
                args.controller_uri,
                self.controller_config["host"],
                int(self.controller_config["port"]),
            )
            self.controller_config["host"] = host
            self.controller_config["port"] = port

        if getattr(args, "metastore_uri", None):
            host, port = split_host_port(
                args.metastore_uri,
                self.metastore.get("host", "localhost"),
                int(self.metastore.get("http_port", 3200)),
            )
            self.metastore["host"] = host
            self.metastore["http_port"] = port

        if getattr(args, "port", None):
            if component_type == "controller":
                self.controller_config["port"] = args.port
            elif component_type == "broker":
                self.broker_config["port"] = args.port
