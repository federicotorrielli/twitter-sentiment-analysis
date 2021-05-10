import glob
from requests import get

from file_manager import dump_toml, read_toml


def create_definitions(datasets: []):
    """
    Foreach Plutchik emotion, it finds words definition from datasets and
    it writes them to toml files.
    @param datasets:
    """
    i = 0
    names = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

    for sentiment in datasets:
        standard_toml_files = preparse_standard_toml_files()
        slang_toml_files = preparse_slang_toml_files()
        dataset_name = names[i]
        print(f"Processing {len(sentiment)} words for {dataset_name} dataset!")
        slang_dict = {}
        definitions_dict = {}

        for word_tuple in sentiment:
            word = word_tuple[0]
            if not word == "true" and not word == "false":
                definition = check_word_existence(word, standard_toml_files)
                if definition == "":
                    definition = check_word_existence(word, slang_toml_files)
                    if definition == "":
                        count = word_tuple[1]
                        if count > 5 and len(word) > 3:
                            definition = get_dictionary_definition(word)
                            print(f"Searching: {word}, used {count} times, definition: {definition[:35]}...")
                            if definition == "":
                                print(f"{word} is probably a slang")
                                definition = get_slang_definition(word)
                                if definition != "":
                                    slang_dict[word] = definition
                            else:
                                definitions_dict[word] = definition
                    else:
                        print(f"{word} was already in the files, adding it to slangs!")
                        slang_dict[word] = definition
                elif not definition == "":
                    print(f"{word} was already in the files, adding it to definitions!")
                    definitions_dict[word] = definition

        dump_toml(f"../Resources/definitions/standard_definitions_{dataset_name}.toml", definitions_dict)
        dump_toml(f"../Resources/definitions/slang_definitions_{dataset_name}.toml", slang_dict)
        i = i + 1


def get_slang_definition(word):
    """
    Returns the English slang word meaning from Urban Dictionary API.
    @param word: the word to search for the definition of
    @return: the word definition
    """
    response = get(f"https://api.urbandictionary.com/v0/define?term={word}")
    response.encoding = 'utf-8'
    if response.ok:
        text = response.json()
        if 'list' in text:
            if len(text['list']) > 0:
                return text['list'][0]['definition']
    return ""


def get_dictionary_definition(word):
    """
    Returns the English word meaning from Free Dictionary API.
    @param word: the word to search for the definition of
    @return: the word definition
    """
    response = get(f"https://api.dictionaryapi.dev/api/v2/entries/en_US/{word}")
    response.encoding = 'utf-8'
    if response.ok:
        text = response.json()
        if len(text) > 0 and 'title' not in text:
            meanings = text[0]['meanings']
            if len(meanings) > 0:
                return meanings[0]['definitions'][0]['definition']
            else:
                return word
    return ""


def preparse_standard_toml_files():
    """
    Returns a list of English standard word meanings.
    @return: standard words definitions
    """
    toml_files = glob.glob("../Resources/definitions/standard*.toml")
    listoffiles = []
    for toml_file in toml_files:
        listoffiles.append(read_toml(toml_file))
    return listoffiles


def preparse_slang_toml_files():
    """
    Returns a list of English slang word meanings.
    @return: slang words definitions
    """
    toml_files = glob.glob("../Resources/definitions/slang*.toml")
    listoffiles = []
    for toml_file in toml_files:
        listoffiles.append(read_toml(toml_file))
    return listoffiles


def check_word_existence(word, parsedfiles):
    """
    Given a word and the parsed files, it returns the word definition if exists.
    @param word: the word to search for the definition of
    @param parsedfiles:
    @return: the word definition if exists
    """
    for parsed in parsedfiles:
        if word in parsed:
            return parsed.get(word)
    return ""
