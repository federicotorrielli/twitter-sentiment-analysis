from glob import glob
from pprint import pprint
from file_manager import count_file_lines, read_file


def get_lexical_filenames():
    """
    Foreach Plutchik emotion, it returns the lexical filenames.
    @return: lexical filenames
    """
    return [glob("../Resources/lexical/Anger/*.txt"), glob("../Resources/lexical/Anticipation/*.txt"),
            glob("../Resources/lexical/Disgust-Hate/*.txt"), glob("../Resources/lexical/Fear/*.txt"),
            glob("../Resources/lexical/Joy/*.txt"), glob("../Resources/lexical/Sadness/*.txt"),
            glob("../Resources/lexical/Surprise/*.txt"), glob("../Resources/lexical/Trust/*.txt")]


def get_lexical_Nlines():
    """
    Foreach Plutchik emotion, it finds # words.
    @return: list of # sentiment words
    """
    sumlist = []
    for index, files in enumerate(get_lexical_filenames()):
        sumlist.append(0)
        for file in files:
            sumlist[index] += count_file_lines(file)
    return sumlist


if __name__ == '__main__':
    pprint(get_lexical_Nlines())
