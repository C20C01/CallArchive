"""Follow-up assignments shared by the console and notifications."""

import re


def caller_key(value):
    number = re.sub(r"[\s()（）-]", "", str(value or ""))
    prefixed = re.fullmatch(r"(?:\+86|0086)(1[0-9]{10})", number)
    return prefixed.group(1) if prefixed else number


def normalize_followup_names(names):
    directory = {}
    for number, name in (names or {}).items():
        key, label = caller_key(number), name.strip()
        if key and label:
            directory[key] = label
    return directory
