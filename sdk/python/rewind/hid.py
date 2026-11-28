"""Linux evdev constants and helpers for deterministic paravirtual HID input."""

from __future__ import annotations

from typing import Dict, List, Tuple

EV_SYN = 0
EV_KEY = 1
EV_REL = 2
EV_ABS = 3

SYN_REPORT = 0
ABS_X = 0
ABS_Y = 1
REL_HWHEEL = 6
REL_WHEEL = 8

BTN_LEFT = 0x110
BTN_RIGHT = 0x111
BTN_MIDDLE = 0x112
KEY_LEFTSHIFT = 42

KEY_NAMES: Dict[str, int] = {
    "esc": 1, "escape": 1,
    "1": 2, "2": 3, "3": 4, "4": 5, "5": 6, "6": 7, "7": 8, "8": 9, "9": 10, "0": 11,
    "minus": 12, "-": 12, "equal": 13, "=": 13, "backspace": 14, "tab": 15,
    "q": 16, "w": 17, "e": 18, "r": 19, "t": 20, "y": 21, "u": 22, "i": 23, "o": 24, "p": 25,
    "[": 26, "]": 27, "enter": 28, "return": 28, "ctrl": 29, "leftctrl": 29,
    "a": 30, "s": 31, "d": 32, "f": 33, "g": 34, "h": 35, "j": 36, "k": 37, "l": 38,
    ";": 39, "'": 40, "`": 41, "shift": 42, "leftshift": 42, "\\": 43,
    "z": 44, "x": 45, "c": 46, "v": 47, "b": 48, "n": 49, "m": 50,
    ",": 51, ".": 52, "/": 53, "rightshift": 54, "alt": 56, "leftalt": 56,
    "space": 57, " ": 57,
    "home": 102, "up": 103, "pageup": 104, "left": 105, "right": 106,
    "end": 107, "down": 108, "pagedown": 109, "delete": 111,
}

SHIFTED_CHARS: Dict[str, int] = {
    "!": 2, "@": 3, "#": 4, "$": 5, "%": 6, "^": 7, "&": 8, "*": 9, "(": 10, ")": 11,
    "_": 12, "+": 13, "{": 26, "}": 27, ":": 39, '"': 40, "~": 41, "|": 43,
    "<": 51, ">": 52, "?": 53,
}

BUTTON_CODES: Dict[str, int] = {
    "left": BTN_LEFT,
    "right": BTN_RIGHT,
    "middle": BTN_MIDDLE,
}


def abs_move_events(
    x: int, y: int, *, width: int = 1280, height: int = 720
) -> List[Tuple[int, int, int]]:
    """Encodes pixel ``(x, y)`` into normalized ``0..32767`` evdev ``EV_ABS`` events."""
    ax = max(0, min(32767, int(round(x * 32767 / max(1, width - 1)))))
    ay = max(0, min(32767, int(round(y * 32767 / max(1, height - 1)))))
    return [
        (EV_ABS, ABS_X, ax),
        (EV_ABS, ABS_Y, ay),
        (EV_SYN, SYN_REPORT, 0),
    ]


def button_code(button: str) -> int:
    code = BUTTON_CODES.get(button.lower())
    if code is None:
        raise ValueError(
            f"unknown mouse button {button!r} (expected left, right, middle)"
        )
    return code


def key_code(key_name: str) -> int:
    code = KEY_NAMES.get(key_name.lower())
    if code is None:
        raise ValueError(f"unknown key name {key_name!r}")
    return code


def char_press_release(
    ch: str,
) -> Tuple[List[Tuple[int, int, int]], List[Tuple[int, int, int]]]:
    """Returns ``(press_events, release_events)`` for one ASCII character."""
    if ch == "\n":
        code = KEY_NAMES["enter"]
        return (
            [(EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)],
            [(EV_KEY, code, 0), (EV_SYN, SYN_REPORT, 0)],
        )
    if ch == "\t":
        code = KEY_NAMES["tab"]
        return (
            [(EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)],
            [(EV_KEY, code, 0), (EV_SYN, SYN_REPORT, 0)],
        )
    if "A" <= ch <= "Z":
        code = KEY_NAMES[ch.lower()]
        return (
            [(EV_KEY, KEY_LEFTSHIFT, 1), (EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)],
            [(EV_KEY, code, 0), (EV_KEY, KEY_LEFTSHIFT, 0), (EV_SYN, SYN_REPORT, 0)],
        )
    if ch in SHIFTED_CHARS:
        code = SHIFTED_CHARS[ch]
        return (
            [(EV_KEY, KEY_LEFTSHIFT, 1), (EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)],
            [(EV_KEY, code, 0), (EV_KEY, KEY_LEFTSHIFT, 0), (EV_SYN, SYN_REPORT, 0)],
        )
    if ch in KEY_NAMES:
        code = KEY_NAMES[ch]
        return (
            [(EV_KEY, code, 1), (EV_SYN, SYN_REPORT, 0)],
            [(EV_KEY, code, 0), (EV_SYN, SYN_REPORT, 0)],
        )
    raise ValueError(f"unsupported ASCII character {ch!r} in type_text")
