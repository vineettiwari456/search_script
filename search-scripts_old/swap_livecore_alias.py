from utils.esutils import ESUtils
from werkzeug.exceptions import abort
from elasticsearch import Elasticsearch
import certifi


class SwapAlias:
    @staticmethod
    def execute():
        # es = ESUtils.esConn()
        # indices = ["yin", "yang"]
        es = Elasticsearch(["http://10.216.240.58:9200"], ca_certs=certifi.where(), timeout=60)
        indices = ["my_index_tarun", "my_index_tarun1"]
        alias = "livecore"
        if es.indices.exists_alias(alias):
            alias_index = list(es.indices.get(alias).keys())[0]
            indices.remove(alias_index)
            new_index = indices[0]

            print("Swapping alias %s from %s to %s" % (alias, alias_index, new_index))
            try:
                es.indices.update_aliases({
                    "actions": [
                        {"remove": {"index": alias_index, "alias": alias}},
                        {"add": {"index": new_index, "alias": alias}},
                    ]
                })
                print("Alias swapped")
                print("Updating replicas settings for %s to 0" % alias_index)
                ESUtils.updateRefreshReplicasSettings(es, alias_index, 0)
                print("Updating replicas settings for %s to 2" % new_index)
                ESUtils.updateRefreshReplicasSettings(es, new_index, 2)
            except Exception as e:
                abort(500, e)
        else:
            print("Alias %s is not present" % alias)


if __name__ == '__main__':
    SwapAlias.execute()