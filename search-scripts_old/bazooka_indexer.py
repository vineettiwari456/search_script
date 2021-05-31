import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import argparse
import json
import logging
import graypy
from elasticsearch import exceptions

from utils.dbutils import DBUtils
from utils.esutils import ESUtils
from utils.constants import GRAYLOG_PORT


def convert_into_dict_of_list(dict_of_set, field=None):
    dict_of_list = {}
    for key, value in dict_of_set.items():
        value_list = list(value)
        for i in range(len(value_list)):
            if field == "comments":
                value_list[i] = json.loads(value_list[i])
        dict_of_list[key] = value_list
    return dict_of_list


def update_set(set_data, update_data, action="update"):
    if isinstance(update_data, list):
        for data in update_data:
            if isinstance(data, dict):
                data = json.dumps(data)
            if action == "update":
                set_data.add(data)
            elif action == "delete":
                if data in set_data:
                    set_data.remove(data)
    else:
        if action == "update":
            set_data.add(update_data)
        elif action == "delete":
            if update_data in set_data:
                set_data.remove(update_data)
    return set_data


class BazookaIndexer:
    def __init__(self, index_name, minutes, hours, prev_timestamp=None):
        self.index_name = index_name
        self.es_conn = ESUtils.esConn()
        # Indexing will be done for new db data recorded after prev time
        if prev_timestamp is None:
            self.prev_time = (
                datetime.now(timezone.utc)
                + timedelta(minutes=30, hours=5)
                - timedelta(minutes=minutes, hours=hours)
            )
        else:
            self.prev_time = datetime.strptime(prev_timestamp, "%Y-%m-%d %H:%M:%S")
        self.indexing_status_logger = logging.getLogger("Bazooka Indexer Logger")
        self.indexing_status_logger.setLevel(logging.DEBUG)
        handler = graypy.GELFTCPHandler("10.216.240.128", GRAYLOG_PORT)
        self.indexing_status_logger.addHandler(handler)

    def check_es_conn(self):
        if not self.es_conn.indices.exists(index=self.index_name):
            self.indexing_status_logger.error("%s INDEX NOT FOUND" % self.index_name)
            raise ValueError("%s INDEX NOT FOUND" % self.index_name)
        print("ES connection established")

    def es_update_query_id(self, user_data, field, action="update"):
        """
        Updates doc by id for given field
        """
        # List of all user ids
        user_ids = list(user_data.keys())
        batch_size = 5000
        es_user_found = []
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i : i + batch_size]
            body = {
                "query": {"terms": {"kiwi_user_id": batch}},
                "_source": ["kiwi_user_id", field],
            }

            # Fetching elastic search data for given doc_ids
            try:
                res = self.es_conn.search(index=self.index_name, body=body, size=batch_size)
            except exceptions.ConnectionTimeout:
                self.indexing_status_logger.error("Elastic Search Connection Timed out while indexing %s" % field)
                continue
            update_data = defaultdict(lambda: set())
            try:
                result = res["hits"]["hits"]
                for doc in result:
                    kiwi_user_id = doc["_source"]["kiwi_user_id"]
                    es_user_found.append(kiwi_user_id)
                    if field in doc["_source"] and doc["_source"][field] is not None:
                        update_data[doc["_id"]] = update_set(
                            update_data[doc["_id"]], doc["_source"][field]
                        )
                    update_data[doc["_id"]] = update_set(
                        update_data[doc["_id"]], user_data[kiwi_user_id], action
                    )
            except KeyError:
                self.indexing_status_logger.debug(
                    "No data for given users in elastic search"
                )
                continue

            # Updating elastic search index
            ESUtils.updatePartialDataInES(
                es_conn=self.es_conn,
                index_name=self.index_name,
                data=convert_into_dict_of_list(update_data, field),
                field=field,
            )
        self.indexing_status_logger.info(
            "Total Documents updated in es : %d" % len(es_user_found)
        )
        if len(user_ids) != len(es_user_found):
            self.indexing_status_logger.warning(
                "Users not found in Elastic Search : \n"
                + ", ".join(map(str, list(set(user_ids) - set(es_user_found))))
            )

    def es_update_query_profile(self, profile_data, field, action="update"):
        """
        Updates doc by kiwi_profile_id
        """
        kiwi_profile_ids = list(profile_data.keys())
        self.indexing_status_logger.info(
            "Total profiles fetched from db for %s %s : %d"
            % (action, field, len(kiwi_profile_ids))
        )
        batch_size = 5000
        es_profile_found = []
        for i in range(0, len(kiwi_profile_ids), batch_size):
            batch = kiwi_profile_ids[i : i + batch_size]
            body = {
                "query": {
                    "nested": {
                        "path": "profiles",
                        "query": {"terms": {"profiles.kiwi_profile_id": list(batch)}},
                        "inner_hits": {"_source": "profiles.kiwi_profile_id"},
                    }
                },
                "_source": field,
            }
            try:
                res = self.es_conn.search(index=self.index_name, body=body, size=batch_size)
            except exceptions.ConnectionTimeout:
                self.indexing_status_logger.error("Elastic Search Connection Timed out while indexing %s" % field)
                continue
            update_data = defaultdict(lambda: set())
            try:
                result = res["hits"]["hits"]
                for doc in result:
                    if field in doc["_source"]:
                        doc["_source"][field] = doc["_source"][field] or []
                        update_data[doc["_id"]] = update_set(
                            update_data[doc["_id"]], doc["_source"][field]
                        )
                    for profile in doc["inner_hits"]["profiles"]["hits"]["hits"]:
                        kiwi_profile_id = profile["_source"]["kiwi_profile_id"]
                        es_profile_found.append(kiwi_profile_id)
                        update_data[doc["_id"]] = update_set(
                            update_data[doc["_id"]],
                            profile_data[kiwi_profile_id],
                            action,
                        )
            except KeyError:
                self.indexing_status_logger.info(
                    "No data for given users in elastic search"
                )
                return
            convert_into_dict_of_list(update_data)
            ESUtils.updatePartialDataInES(
                es_conn=self.es_conn,
                index_name=self.index_name,
                data=convert_into_dict_of_list(update_data, field),
                field=field,
            )
        self.indexing_status_logger.info(
            "Total profiles updated in es : %d" % len(es_profile_found)
        )
        if len(kiwi_profile_ids) != len(es_profile_found):
            self.indexing_status_logger.warning(
                "Profiles not found in Elastic Search : \n"
                + ", ".join(
                    map(str, list(set(kiwi_profile_ids) - set(es_profile_found)))
                )
            )

    def update_profile_data(self, db_conn, sql_query, field, **kwargs):
        db_data = DBUtils.fetch_results_in_batch(
            db_conn, sql_query, -1, dictionary=True
        )
        profile_viewed = defaultdict(lambda: [])
        for row in db_data:
            if row["kiwi_profile_id"] is not None:
                profile_viewed[row["kiwi_profile_id"]].append(row[field])
        return self.es_update_query_profile(
            profile_data=profile_viewed, field=field, **kwargs
        )

    def index_profile_downloaded_by(self):
        self.indexing_status_logger.info("\nIndexing Profile Downloaded by : ")
        prev_date = datetime.now() - timedelta(1)
        prev_date = prev_date.strftime("%Y-%m-%d %H:%M:%S")
        sql_query = (
            "select resid as kiwi_profile_id, subuid as downloaded_by from bazooka.download_resume where download_date > '%s';"
            % prev_date
        )
        return self.update_profile_data(
            db_conn=DBUtils.bazooka3_connection(),
            sql_query=sql_query,
            field="downloaded_by",
        )

    def index_profile_viewed_by(self):
        self.indexing_status_logger.info("\nIndexing Profile Viewed by :")
        sql_query = (
            "select resid as kiwi_profile_id, subuid as viewed_by from bazooka.resumeviewed where viewdate > '%s';"
            % self.prev_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        return self.update_profile_data(
            db_conn=DBUtils.bazooka3_connection(),
            sql_query=sql_query,
            field="viewed_by",
        )

    def index_profile_comments(self):
        self.indexing_status_logger.info("\nIndexing Profile Comments :")

        def fetch_comments(action):
            sql_query = (
                "select resumeid as kiwi_profile_id, subuid, comment, commentdate from bazooka.corp_comments where status='%s' and updatedate > '%s'"
                % (action, self.prev_time.strftime("%Y-%m-%d %H:%M:%S"))
            )
            db_data = DBUtils.fetch_results_in_batch(
                DBUtils.bazooka1_connection(), sql_query, -1, dictionary=True
            )
            profile_comments = defaultdict(lambda: [])
            for row in db_data:
                if row["kiwi_profile_id"] is not None:
                    profile_comments[row["kiwi_profile_id"]].append(
                        {
                            "commented_by": row["subuid"],
                            "comment": row["comment"],
                            "comment_date": str(row["commentdate"]),
                        }
                    )
            return profile_comments

        self.es_update_query_profile(fetch_comments("active"), "comments")
        self.es_update_query_profile(
            fetch_comments("deleted"), "comments", action="delete"
        )

    def index_profile_follows(self):
        self.indexing_status_logger.info("\nIndexing Profile Follows :")
        sql_add_query = (
            "select seeker_profile_id as kiwi_profile_id, recruiter_profile_id as follows from bazooka.rec_social_follow where status= '1' and block = '0' and updated > '%s' ;"
            % self.prev_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        self.update_profile_data(
            db_conn=DBUtils.bazooka1_connection(),
            sql_query=sql_add_query,
            field="follows",
            action="update",
        )

        sql_remove_query = (
            "select seeker_profile_id as kiwi_profile_id, recruiter_profile_id as follows from bazooka.rec_social_follow where status= '2' and block = '0' and updated > '%s' ;"
            % self.prev_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        self.update_profile_data(
            db_conn=DBUtils.bazooka1_connection(),
            sql_query=sql_remove_query,
            field="follows",
            action="delete",
        )
    #NIU
    def index_seeker_services(self):
        self.indexing_status_logger.info("\nIndexing seeker services : ")
        sql_query = (
            "select uid, channel_id, subchannel_id, type from bazooka.seeker_brandings where updated > '%s' and enabled = '1';"
            % self.prev_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        db_data = DBUtils.fetch_results_in_batch(
            DBUtils.bazooka2_connection(), sql_query, -1, dictionary=True
        )
        seeker_services = defaultdict(lambda: [])
        for row in db_data:
            seeker_services[row["uid"]].append(row["type"] + str(row["subchannel_id"]))
        self.indexing_status_logger.info(
            "Total Users fetched from db (Update services) : %d" % len(seeker_services)
        )
        self.es_update_query_id(user_data=seeker_services, field="seeker_services")

        sql_del_query = (
            "select uid, channel_id, subchannel_id, type from bazooka.seeker_brandings where updated > '%s' and enabled = '0';"
            % self.prev_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        db_data = DBUtils.fetch_results_in_batch(
            DBUtils.bazooka2_connection(), sql_del_query, -1, dictionary=True
        )
        seeker_services = defaultdict(lambda: [])
        for row in db_data:
            seeker_services[row["uid"]].append(row["type"] + str(row["subchannel_id"]))
        self.indexing_status_logger.info(
            "Total Users fetched from db (Remove services): %d" % len(seeker_services)
        )
        self.es_update_query_id(
            user_data=seeker_services, field="seeker_services", action="delete"
        )

    def index_invitation_sent_by(self):
        self.indexing_status_logger.info("\nIndexing Invitation sent By : ")
        sql_query = (
            "select uid, recruiter_profile_id, subchannel_id from bazooka.social_follow_notification where create_date > '%s';"
            % self.prev_time.strftime("%Y-%m-%d")
        )
        db_data = DBUtils.fetch_results_in_batch(
            DBUtils.bazooka1_connection(), sql_query, -1, dictionary=True
        )
        user_social_follow_notifications = defaultdict(lambda: [])
        for row in db_data:
            user_social_follow_notifications[row["uid"]].append(
                row["recruiter_profile_id"]
            )
        self.indexing_status_logger.info(
            "Total Users fetched from db : %d" % len(user_social_follow_notifications)
        )
        return self.es_update_query_id(
            user_data=user_social_follow_notifications, field="invitation_sent_by"
        )

    def index(self):
        self.index_invitation_sent_by()
        self.index_seeker_services()
        self.index_profile_viewed_by()
        self.index_profile_follows()
        self.index_seeker_services()
        self.index_profile_downloaded_by()
        self.index_profile_comments()


if __name__ == "__main__":

    start_time = datetime.now()
    print("Starting Bazooka indexer at", start_time)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--index-name", default="users", help="index name", required=True
    )
    parser.add_argument(
        "--hours", "--hours", help="Number of prev hours to check", type=int, default=0
    )
    parser.add_argument(
        "--mins", "--mins", help="Number of prev mins to check", type=int, default=0
    )
    parser.add_argument(
        "-t", "--timestamp", help="Prev Timestamp", type=str, default=None
    )
    argv = vars(parser.parse_args())

    index = argv["index_name"]
    no_of_hour = argv.get("hours")
    no_of_min = argv.get("mins")
    timestamp = argv.get("timestamp")
    bazooka_indexer = BazookaIndexer(
        index_name=index, minutes=no_of_min, hours=no_of_hour, prev_timestamp=timestamp
    )

    if "env" in os.environ:
        bazooka_indexer.indexing_status_logger.info("Environment : " + os.environ.get("env"))
        print("Environment : ", os.environ.get("env"))
    else:
        bazooka_indexer.indexing_status_logger.warning("No defined Environment")
        print("No defined Environment")
    bazooka_indexer.check_es_conn()
    bazooka_indexer.index()

    end_time = datetime.now()
    print("Bazooka indexer completed at ", end_time)
    print("Total Time taken : ", end_time - start_time)
