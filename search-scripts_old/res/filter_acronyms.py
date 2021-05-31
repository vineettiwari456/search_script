from collections import defaultdict

from elasticsearch.client import IndicesClient
from tqdm import tqdm

from utils.esutils import ESUtils

with open("index_synonyms.txt", "r") as f:
    content = f.readlines()


def get_acronym(text):
    return "".join(map(lambda x: x[0], text.split()))


es = ESUtils.esConn()

with open("index_acronyms.txt", "w") as f2:
    for line in tqdm(content):
        words = list(map(lambda x: x.strip(), line.split(",")))
        acronyms = list(map(get_acronym, words))
        mapping = defaultdict(list)
        for w, a in zip(words, acronyms):
            mapping[a].append(w)
        conflict = next(filter(lambda a: a in words, acronyms), None)
        if conflict:
            tokens = set(
                map(
                    lambda x: x["token"],
                    filter(
                        lambda x: x["type"] == "word",
                        IndicesClient(es).analyze(
                            "search_jenkins_50000", {"text": conflict}
                        )["tokens"],
                    ),
                )
            )
            if conflict[:-1] in tokens:
                f2.write(
                    "{} ({}) = {}\n".format(
                        conflict, tokens, ",".join(mapping[conflict])
                    )
                )
