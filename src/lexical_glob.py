from glob import glob
from src.file_manager import count_file_lines, get_project_root


def get_lexical_filenames():
    """
    Foreach Plutchik emotion, it returns the lexical filenames.
    @return: lexical filenames
    """
    return [glob(f"{get_project_root()}/Resources/lexical/Anger/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Anticipation/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Disgust-Hate/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Fear/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Joy/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Sadness/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Surprise/*.txt"),
            glob(f"{get_project_root()}/Resources/lexical/Trust/*.txt")]


def get_lexical_Nlines():
    """
    Foreach Plutchik emotion file, it finds # words.
    @return: list of # sentiment words
    """
    sumlist = []
    for index, files in enumerate(get_lexical_filenames()):
        sumlist.append(0)
        for file in files:
            sumlist[index] += count_file_lines(file)
    return sumlist


if __name__ == '__main__':
    print(get_lexical_Nlines())
