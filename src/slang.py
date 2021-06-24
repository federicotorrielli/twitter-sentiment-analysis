import glob

from requests import get

from src.file_manager import dump_toml, get_project_root, read_toml


def create_definitions(datasets: [], dao):
    """
    Foreach Plutchik emotion, it finds words definition from datasets and
    it writes them to toml files.
    @param dao:
    @param datasets:
    """
    i = 0
    sentiments = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

    for sentiment in datasets:
        standard_toml_files = preparse_standard_toml_files()
        slang_toml_files = preparse_slang_toml_files()
        dataset_name = sentiments[i]
        print(f"Processing {len(sentiment)} words for {dataset_name} dataset!")
        slang_dict = {}
        definitions_dict = {}

        for word, count in sentiment.items():
            if not word == "true" and not word == "false" and not word == "_id" \
                    and "." not in word and "$" not in word:
                definition = check_word_existence(word, standard_toml_files)
                if definition == "":
                    definition = check_word_existence(word, slang_toml_files)
                    if definition == "":
                        if count > 5 and len(word) > 3:
                            definition = get_dictionary_definition(word)
                            print(f"Searching: {word}, used {count} times, definition: {definition[:35]}...")
                            if definition == "":
                                print(f"{word} is probably a slang")
                                definition = get_slang_definition(word)
                                if definition != "":
                                    slang_dict[word] = definition
                                else:
                                    slang_dict[word] = "no definition found."
                            else:
                                definitions_dict[word] = definition
                    else:
                        print(f"{word} was already in the files, adding it to slangs!")
                        slang_dict[word] = definition
                elif not definition == "":
                    print(f"{word} was already in the files, adding it to definitions!")
                    definitions_dict[word] = definition

        dump_toml(f"{get_project_root()}/Resources/definitions/standard_definitions_{dataset_name}.toml",
                  definitions_dict)
        dump_toml(f"{get_project_root()}/Resources/definitions/slang_definitions_{dataset_name}.toml", slang_dict)
        # TODO: why should I update meanings upon the db only after building toml files already builded?
        if dao.is_mongodb():
            dao.dump_definitions(definitions_dict, f"standard_definitions_{dataset_name}")
            dao.dump_definitions(slang_dict, f"slang_definitions_{dataset_name}")
        i = i + 1


def get_slang_definition(word):
    """
    Returns the English slang word meaning from Urban Dictionary API.
    @param word: the word to search for the definition of
    @return: the word definition
    """
    response = get(f"https://api.urbandictionary.com/v0/define?term={word}")
    response.encoding = 'utf-8'
    good_definition = {"definition": "", "count": 0}
    if response.ok:
        text = response.json()
        if 'list' in text:
            for elem in text['list']:
                if elem['thumbs_up'] > elem['thumbs_down'] and good_definition['count'] < elem['thumbs_up']:
                    good_definition = {"definition": elem['definition'], "count": elem['thumbs_up']}
    return good_definition["definition"]


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
    toml_files = glob.glob(f"{get_project_root()}/Resources/definitions/standard*.toml")
    listoffiles = []
    for toml_file in toml_files:
        listoffiles.append(read_toml(toml_file))
    return listoffiles


def preparse_slang_toml_files():
    """
    Returns a list of English slang word meanings.
    @return: slang words definitions
    """
    toml_files = glob.glob(f"{get_project_root()}/Resources/definitions/slang*.toml")
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
