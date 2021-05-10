import os
import subprocess
from pathlib import Path

import toml


def count_file_lines(path: str) -> int:
    """
    Counting lines in a file, we use two different methods
    depending on the operating system used. If on Linux/MacOS
    we call the fast wc for counting the file lines.
    If on Windows (nt) we read the file and sum 1 for every line.
    @param path: path to the file
    @return: # of lines in a file
    """
    if os.name == 'nt':
        return sum(1 for _ in read_file(path))
    else:
        return int(subprocess.check_output(f"/usr/bin/wc -l {path}", shell=True).split()[0])


def read_file(path: str) -> str:
    """
    Returns the file content using utf-8 encoding.
    @param path: path to the file
    @return: file content
    """
    with open(path, 'r', encoding="utf-8") as file:
        return file.read()


def write_file(path: str, data: [str]):
    """
    Writes a file using utf-8 encoding.
    @param path: path to the file
    @param data: list of rows
    """
    with open(path, 'w', encoding="utf-8") as file:
        file.writelines(data)


def append_file(path: str, data: [str]):
    """
    Appends data to the file using utf-8 encoding.
    @param path: path to the file
    @param data: list of rows
    """
    with open(path, 'a', encoding="utf-8") as file:
        file.write(data)


def dump_toml(path: str, data):
    """
    Writes out data to toml file using utf-8 encoding.
    @param path: path to the toml file
    @param data: list of rows
    """
    with open(path, 'w', encoding="utf-8") as file:
        file.truncate(0)
        toml.dump(data, file)


def read_toml(path: str):
    """
    Returns the toml file content using utf-8 encoding.
    @param path: path to the toml file
    @return: toml file content
    """
    with open(path, 'r', encoding="utf-8") as file:
        return toml.load(file)


def get_project_root():
    """
    Returns project root folder.
    @return: project root path
    """
    return Path(__file__).parent.parent
