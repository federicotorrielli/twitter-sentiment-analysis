from pathlib import Path

import toml


def read_file(path: str) -> str:
    with open(path, 'r', encoding="utf-8") as file:
        return file.read()


def write_file(path: str, data: [str]):
    with open(path, 'w', encoding="utf-8") as file:
        file.writelines(data)


def append_file(path: str, data: [str]):
    with open(path, 'a', encoding="utf-8") as file:
        file.write(data)


def dump_toml(path: str, data):
    with open(path, 'w', encoding="utf-8") as file:
        file.truncate(0)
        toml.dump(data, file)


def read_toml(path: str):
    with open(path, 'r', encoding="utf-8") as file:
        return toml.load(file)


def get_project_root():
    """Returns project root folder."""
    return Path(__file__).parent.parent
