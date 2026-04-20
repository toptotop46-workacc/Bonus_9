import sys
import datetime

# Включается из config.toml: debug_logging = true
_debug_enabled: bool = False


def set_debug_enabled(enabled: bool) -> None:
    global _debug_enabled
    _debug_enabled = bool(enabled)


def is_debug_enabled() -> bool:
    return _debug_enabled


def apply_config(cfg: dict) -> None:
    """Включить/выключить logger.debug по ключу debug_logging в config.toml."""
    set_debug_enabled(bool(cfg.get("debug_logging", False)))


RESET  = "\033[0m"
WHITE  = "\033[97m"
ORANGE = "\033[38;5;214m"
GREEN  = "\033[92m"
RED    = "\033[91m"
GREY   = "\033[90m"
CYAN   = "\033[96m"


def _log(level: str, color: str, msg: str) -> None:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{color}{ts} | {level:<7} | {msg}{RESET}", file=sys.stderr)


def info(msg: str)    -> None: _log("INFO",    WHITE,  msg)
def warning(msg: str) -> None: _log("WARNING", ORANGE, msg)
def success(msg: str) -> None: _log("SUCCESS", GREEN,  msg)
def error(msg: str)   -> None: _log("ERROR",   RED,    msg)
def debug(msg: str) -> None:
    if not _debug_enabled:
        return
    _log("DEBUG", GREY, msg)
def header(msg: str)  -> None: _log("HEADER",  CYAN,   msg)
