from pathlib import Path


def read_file(path: str) -> str:
    with open(path, 'r', encoding="utf-32") as file:
        return file.read()


def write_file(path: str, data: [str]):
    with open(path, 'w', encoding="utf-32") as file:
        file.writelines(data)


def get_project_root():
    """Returns project root folder."""
    return Path(__file__).parent.parent
