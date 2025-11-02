import os
import configparser
from typing import Any


class EnvConfigParser(configparser.ConfigParser):
    """Interpolates environment variables (e.g., $VAR or ${VAR})."""

    def get(
        self,
        section: str,
        option: str,
        *,
        raw: bool = False,
        vars: Any = None,
        fallback: Any = None,
    ) -> Any:
        val = super().get(section, option, raw=raw, vars=vars, fallback=fallback)

        if val is None:
            return fallback

        if isinstance(val, str):
            return os.path.expandvars(val)
        return val
