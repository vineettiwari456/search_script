from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError
import json
import os
import certifi
import sys
import requests
# sys.path.append("/home/ashwin.euler/code_restructured_new/utils/")
from . import constants
from .constants import AWS_ES_END_POINT, YANG, YIN
from .stored_script_queries import STORED_SCRIPTS_QUERIES, STORED_SCRIPTS_END_POINT

env = os.environ.get("env")


class ESUtils:

    def esConn():
        if env == "PROD":
            return Elasticsearch([AWS_ES_END_POINT], use_ssl=True, ca_certs=certifi.where(), timeout=60)
        else:
            return Elasticsearch([AWS_ES_END_POINT], ca_certs=certifi.where(), timeout=60)

    def createESIndex(es_conn, index_name, force_create):
        schema_file = os.path.dirname(os.path.abspath(__file__)) + "/../res/schema.json"
        synonyms_file = os.path.dirname(os.path.abspath(__file__)) + "/../res/index_synonyms.txt"
        search_synonyms_file = os.path.dirname(os.path.abspath(__file__)) + "/../res/search_synonyms.txt"
        with open(synonyms_file, "r", encoding='utf-8') as f:
            synonyms_lines = [line[:-1] for line in f.readlines()]
        with open(search_synonyms_file, "r", encoding='utf-8') as f:
            search_synonyms_lines = [line[:-1] for line in f.readlines()]
        schema = json.load(open(schema_file))
        schema["settings"]["analysis"]["filter"]["synonym"]["synonyms"] = synonyms_lines
        schema["settings"]["analysis"]["filter"]["synonym_graph"]["synonyms"] = synonyms_lines
        schema["settings"]["analysis"]["filter"]["search_synonym"]["synonyms"] = search_synonyms_lines
        print("Creating index")
        try:
            es_conn.indices.create(index=index_name, body=schema)
        except RequestError as e:
            print(e)
            if force_create:
                print("Deleting the previous index because --force-option=true")
                es_conn.indices.delete(index=index_name)
                print("Done Deleting the previous index")
                ESUtils.createESIndex(es_conn, index_name, force_create)
            else:
                print("Could not create index, index already present")
        else:
            print("Done creating index")

    def updateRefreshReplicasSettings(es_conn, index_name, number_of_replicas=1):
        settings = {
            "index": {
                "number_of_replicas": number_of_replicas,
                "refresh_interval": "60s"
            }
        }
        try:
            es_conn.indices.put_settings(body=settings, index=index_name)
            print("Updated refresh and replicas settings")
        except RequestError as e:
            print(e)
            print("Failed to update refresh and replicas settings")

    def indexDataInES(es_conn, index_name, data):
        actions = [
            {
                "_index": index_name,
                "_type": "_doc",
                "_id": str(user["user_id"]),
                "_source": user
            }
            for user in data
        ]
        bulk(es_conn, actions)
        return True

    def updateDataInES(es_conn, index_name, data):
        actions = [
            {
                "_index": index_name,
                "_type": "_doc",
                "_id": str(user_id),
                "_source": {"doc": {"profiles": profiles}},
                "_op_type": "update"
            }
            for user_id, profiles in data.items()
        ]
        bulk(es_conn, actions, raise_on_error=False)
        return True

    def updatePartialDataInES(es_conn, index_name, data, field):
        actions = [
            {
                "_index": index_name,
                "_type": "_doc",
                "_id": str(user_id),
                "_source": {"doc": {field: value}},
                "_op_type": "update"
            }
            for user_id, value in data.items()
        ]
        bulk(es_conn, actions, raise_on_error=True)
        return True

    def get_index_from_alias(es_conn, alias):
        if es_conn.indices.exists_alias(alias):
            response = es_conn.indices.get_alias(index=alias)
            for index, index_aliases in response.items():
                return index
        return None

    def get_active_inactive_indexes(es_conn, alias):
        active_index = ESUtils.get_index_from_alias(es_conn, alias)
        if not active_index:
            active_index = YIN
        return {'active_index': active_index, 'inactive_index': YIN if active_index == YANG else YANG}

    def add_stored_scripts(es_conn):
        store_scripts_json = requests.get(AWS_ES_END_POINT + STORED_SCRIPTS_END_POINT).json()
        existing_stored_scripts_ids = list(store_scripts_json["metadata"]["stored_scripts"].keys()) \
            if store_scripts_json.get("metadata") and store_scripts_json["metadata"].get("stored_scripts") else []
        to_be_add_stored_scripts_ids = list(set(list(STORED_SCRIPTS_QUERIES.keys())) - set(existing_stored_scripts_ids))
        for script_id in to_be_add_stored_scripts_ids:
            es_conn.put_script(id=script_id, body=STORED_SCRIPTS_QUERIES[script_id])
