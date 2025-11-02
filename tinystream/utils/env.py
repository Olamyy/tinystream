import os


def env_default(env_var, *, default=None):
    """Return a function that fetches an env var or falls back to default."""

    def _factory():
        return os.getenv(env_var, default)

    return _factory


def split_host_port(s: str):
    host, port_str = s.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError:
        return s, None
    else:
        return host, port
