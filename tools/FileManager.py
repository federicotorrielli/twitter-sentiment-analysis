from pathlib import Path


class FileManager:
    @staticmethod
    def read(path: str) -> str:
        with open(path, 'r', encoding="utf-32") as file:
            return file.read()

    @staticmethod
    def write(path: str, data: [str]):
        with open(path, 'w', encoding="utf-32") as file:
            file.writelines(data)


def get_project_root():
    """Returns project root folder."""
    return Path(__file__).parent.parent
