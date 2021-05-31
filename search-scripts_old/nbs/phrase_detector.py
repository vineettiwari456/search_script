import re


class TrieNode():
    """
    Our trie node implementation. Very basic. but does the job
    """

    def __init__(self, keyword: str):
        self.keyword = keyword
        self.children = {}
        # Is it the last keywordacter of the word.`
        self.phrase_finished = False
        # This will be filled if phrase_finished = True
        self.phrase = None
        # How many times this keywordacter appeared in the addition process
        self.counter = 1
        self.type = []
        self.type_counter = {}

    def add(self, root, exploded_phrase, phrase_type=None):
        """
        Adding a phrase in the trie structure
        """
        node = root
        for keyword in exploded_phrase:
            # Search for the keywordacter in the children of the present `node`
            if keyword in node.children:
                # We found it, increase the counter by 1 to keep track that another
                # word has it as well
                node.children[keyword].counter += 1
                # And point the node to the child that contains this keyword
                node = node.children[keyword]
            # We did not find it so add a new chlid
            else:
                new_node = TrieNode(keyword)
                node.children[keyword] = new_node
                # And then point node to the new child
                node = new_node

            # Maintaining type and counter of each type for node
            if phrase_type:
                if phrase_type not in node.type:
                    node.type.append(phrase_type)
                    node.type_counter[phrase_type] = 1
                else:
                    node.type_counter[phrase_type] += 1
        # Everything finished. Mark it as the end of a phrase.
        node.phrase_finished = True
        node.counter += 1
        if phrase_type:
            if phrase_type not in node.type:
                node.type.append(phrase_type)
                node.type_counter[phrase_type] = 1
            else:
                node.type_counter[phrase_type] += 1

        node.phrase = " ".join(exploded_phrase)

    def find_all_prefixes(self, root, exploded_prefix):
        """
        Check and return
          1. If the prefix exsists in any of the phrases we added so far
          2. If yes then how may phrases actually have the prefix
        """
        prefixes = []
        node = root
        # If the root node has no children, then return False.
        # Because it means we are trying to search in an empty trie
        break_position = len(exploded_prefix)
        if node.children:
            for counter, keyword in enumerate(exploded_prefix):
                if keyword in node.children:
                    if (
                        node.children[keyword].phrase_finished
                        and node.children[keyword].phrase
                    ):
                        prefixes.append(
                            (
                                node.children[keyword].phrase,
                                node.children[keyword].counter,
                                node.children[keyword].type_counter,
                            )
                        )
                    node = node.children[keyword]
                else:
                    break_position = counter
                    break

        return prefixes, break_position


class Trie:
    def __init__(self):
        self.root = TrieNode("*")

    def add(self, exploded_phrase, **kwargs):
        self.root.add(self.root, exploded_phrase, **kwargs)

    def find_all_prefixes(self, phrase):
        return self.root.find_all_prefixes(self.root, phrase)

    def find_one_word_phrases(self):
        one_word_phrases = self.root.children.keys()
        return one_word_phrases


class PhrasesDetector:
    LEVELS = [
        ["fresher"],
        [
            "developer",
            "programmer",
            "engineer",
            "executive",
            "technician",
            "operator",
            "agent",
            "intern",
            "consultant",
            "clerk",
            "designer",
            "engg",
        ],
        [
            "manager",
            "head",
            "lead",
            "leader",
            "director",
            "supervisor",
            "coordinator",
            "vp",
            "gm",
            "administrator",
            "associate",
            "co-ordinator",
            "co ordinator",
            "chief",
            "md",
            "vice president",
            "general manager",
            "mgr",
        ],
    ]

    LEVEL_ZERO_PIPED = "|".join(LEVELS[0])
    LEVEL_ONE_PIPED = "|".join(LEVELS[1])
    LEVEL_TWO_PIPED = "|".join(LEVELS[2])

    ALL_LEVELS = "|".join([ele for level in LEVELS for ele in level])

    def __init__(self):
        self.trie = Trie()

    def add(self, phrase, **kwargs):
        self.trie.add(phrase.split(), **kwargs)

    def prepare_model(self, file_path, **kwargs):
        with open(file_path) as file:
            phrases = file.readlines()  # .decode('utf-8')
            for phrase in phrases:
                self.add(phrase, **kwargs)

    def _find_phrases_stepping(self, exploded_sentence):
        previous_break_position = 0
        phrases = []
        while previous_break_position < len(exploded_sentence):
            prefixes, break_position = self.trie.find_all_prefixes(
                exploded_sentence[previous_break_position:]
            )
            if break_position:
                previous_break_position += break_position
            else:
                previous_break_position += 1
            phrases += prefixes
        return list(phrases)

    def _find_phrases_incremental(self, exploded_sentence):
        # previous_break_position = 0
        phrases = []
        for i in range(len(exploded_sentence)):
            prefixes, break_position = self.trie.find_all_prefixes(
                exploded_sentence[i:]
            )
            phrases += prefixes
        return list(phrases)

    def _get_phrase_role_level(self, sentence):
        with_skills, exclude_roles = True, []
        if re.search(r"\b(" + self.LEVEL_ZERO_PIPED + r")\b", sentence) or re.search(
            r"\b(" + self.LEVEL_ONE_PIPED + r")\b", sentence
        ):
            exclude_roles = self.LEVELS[2]
        elif re.search(r"\b(" + self.LEVEL_TWO_PIPED + r")\b", sentence):
            with_skills = False

        return with_skills, exclude_roles

    def find_phrases(self, sentence, algo="incremental"):
        sentence = sentence.lower()
        with_skills, exclude_roles = self._get_phrase_role_level(sentence)
        exploded_sentence = sentence.split()
        if algo == "incremental":
            phrases = self._find_phrases_incremental(exploded_sentence)
        elif algo == "stepping":
            phrases = self._find_phrases_stepping(exploded_sentence)
        else:
            raise Exception("Algo not found")

        _filtered_phrases = []
        if not with_skills:
            for phrase in phrases:
                if re.search(r"\b(" + self.ALL_LEVELS + r")\b", phrase[0]):
                    _filtered_phrases.append(phrase)

        else:
            _role_phrases, _skill_phrases = [], []
            for phrase in phrases:
                if re.search(r"\b(" + self.ALL_LEVELS + r")\b", phrase[0]):
                    _role_phrases.append(phrase)
                else:
                    _skill_phrases.append(phrase)

            skill_lens = [len(phrase[0].split()) for phrase in _skill_phrases]
            skill_max_len = max(skill_lens) if skill_lens else 0
            if skill_max_len >= 3:
                _skill_phrases = list(
                    filter(lambda x: len(x[0].split()) >= 2, _skill_phrases)
                )

            _filtered_phrases = _skill_phrases + _role_phrases

        filtered_phrases = []
        for phrase in _filtered_phrases:
            if not phrase[0] in self.ALL_LEVELS:
                filtered_phrases.append(phrase)
        if filtered_phrases:
            phrase_max_count = max(
                list(
                    map(
                        lambda x: x[1],
                        list(filter(lambda x: len(x[0].split()) > 1, filtered_phrases)),
                    )
                ),
                default=0,
            )
            filtered_phrases = list(
                filter(
                    lambda x: len(x[0].split()) > 1 or x[1] > phrase_max_count,
                    filtered_phrases,
                )
            )
        filtered_phrases = (
            filtered_phrases
            if sentence in [phrase[0] for phrase in filtered_phrases]
            else filtered_phrases + [sentence]
        )
        return {
            "phrases": [(phrase[0], phrase[1]) for phrase in filtered_phrases],
            "exclude": exclude_roles,
        }
