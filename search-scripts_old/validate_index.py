from datetime import datetime, timedelta, date
from decimal import Decimal
from collections import defaultdict
import argparse
import json

from utils.dbutils import DBUtils
from utils.esutils import ESUtils
from index_parallel_optimized import getUsersData


def modify_data_type(data):
    if isinstance(data, datetime) or isinstance(data, date):
        return data.isoformat()
    if isinstance(data, Decimal):
        return float(data)
    return data


def check_for_empty_list_dict(d1, d2):
    if d1 is None and ((type(d2) == dict or type(d2) == list) and not d2):
        return True
    if d2 is None and ((type(d1) == dict or type(d1) == list) and not d1):
        return True
    return False


def match_data(data1, data2):
    total_checked = 0
    total_matched = 0
    if check_for_empty_list_dict(data1, data2):
        return 1, 1
    data1 = modify_data_type(data1)
    data2 = modify_data_type(data2)
    if type(data1) != type(data2):
        return 1, 0
    if isinstance(data1, dict):
        for key in set(list(data1.keys()) + list(data2.keys())):
            if key in data2.keys() and key in data1.keys():
                checked, matched = match_data(data1[key], data2[key])
                total_matched += matched
                total_checked += checked
            else:
                if key in data2:
                    total_checked += len(data2)
                else:
                    total_checked += len(data1)
    else:
        total_checked += 1
        if data1 == data2:
            total_matched += 1
    return total_checked, total_matched


class ValidateIndex:
    def __init__(self, index_name, days, hours, mins):
        self.mismatch = defaultdict(list)
        self.user_ids = []
        self.index_name = index_name
        self.days = days
        self.hours = hours
        self.mins = mins
        self.checked = {}
        self.matched = {}
        self.users_dict = {}

    def get_user_randomly(self):
        curr_time = datetime.now() - timedelta(minutes=2)
        print("Start timestamp delta: %s\n" % (
            (curr_time - timedelta(days=self.days, hours=self.hours, minutes=self.mins)).strftime('%Y-%m-%d %H:%M:%S')))
        print("End timestamp: %s\n" % (curr_time.strftime('%Y-%m-%d %H:%M:%S')))
        sql_query = (
                "select id, kiwi_user_id from users where kiwi_user_id is not NULL and created_at < %d and created_at > %d ;"
                % (
                    curr_time.timestamp() * 1000,
                    (
                            curr_time
                            - timedelta(days=self.days, hours=self.hours, minutes=self.mins)
                    ).timestamp()
                    * 1000,
                )
        )
        print("fetching all users from database for given creation date")
        users_dict = DBUtils.fetch_results_in_batch(
            DBUtils.falcon_connection(), sql_query, -1, dictionary=True
        )
        self.users_dict = users_dict
        self.user_ids = [user["id"] for user in users_dict]
        print("printing user ids: %s" % str(self.user_ids))
        print("Done")

    def get_user_data(self):
        # Getting Data of users from database
        print("Getting Data of users from database")
        db_user_ids = "(" + ",".join([str(elem) for elem in self.user_ids]) + " )"
        db_user_data = getUsersData(user_ids=db_user_ids, user_range=None)
        print("Done")

        user_ids_str = [str(user) for user in self.user_ids]

        # Getting Data of users from Elastic Search
        print("Getting Data of users from Elastic Search")
        es_conn = ESUtils.esConn()

        query = {"query": {"ids": {"values": user_ids_str}}}
        es_data = es_conn.search(
            index=self.index_name, body=query, size=len(user_ids_str)
        )
        print("Done")
        es_data = es_data["hits"]["hits"]
        es_user_data = {}
        for data in es_data:
            es_user_data[int(data["_id"])] = data["_source"]

        # db_user_data = es_user_data

        return es_user_data, db_user_data

    def save_error(self, key, checked, matched, data):
        if key not in self.checked:
            self.checked[key] = 0
        if key not in self.matched:
            self.matched[key] = 0
        self.checked[key] += checked
        self.matched[key] += matched
        if checked != matched:
            self.mismatch[key].append(data)

    def check_image_url(self, db_url, es_url, user_id):
        if db_url is None:
            if es_url is None:
                self.save_error("image_url", 1, 1, None)
                return 1, 1
            return 1, 0
        elif es_url is not None:
            if db_url == es_url:
                self.save_error("image_url", 1, 1, None)
                return 1, 1
            db_image_url = db_url.split("/")[-2]
            es_image_url = es_url.split("/")[-1].split('.')[0]
            if db_image_url == es_image_url:
                self.save_error("image_url", 1, 1, None)
                return 1, 1
        mismatch_data = {"user_id": user_id, "Database data": db_url, "ES data": es_url}
        self.save_error("image_url", 1, 0, mismatch_data)
        return 1, 0

    def match_preferred_roles(self, user_id, db_list, es_list):
        total_matched = 0
        total_checked = 0
        if db_list is None:
            if es_list is not None:
                print("Found Extra data in elastic search")
                return 1, 0
            return 1, 1
        if es_list is None:
            self.mismatch[user_id].append(db_list)
            return len(db_list), 0
        for db_dict in db_list:
            flag = False
            for data in es_list:
                try:
                    if data["function"]["uuid"] == db_dict["function"]["uuid"]:
                        checked, matched = match_data(
                            db_dict["function"], data["function"]
                        )
                        if checked == matched:
                            total_checked += checked
                            total_matched += matched
                            checked, matched = self.match_list(
                                user_id, db_dict["roles"], data["roles"], "uuid"
                            )
                            total_checked += checked
                            total_matched += matched
                            flag = True
                        break
                except KeyError:
                    total_checked += 1
            if flag is False:
                self.mismatch[user_id].append(db_dict)
                total_checked += len(db_dict)
        return total_checked, total_matched

    def match_list_of_dict_by_key(self, user_id, db_list, es_list, key):
        total_checked = 0
        total_matched = 0
        db_list = db_list or []
        es_list = es_list or []
        for db_dict in db_list:
            flag = 0
            for data in es_list:
                try:
                    if data[key] == db_dict[key]:
                        checked, matched = match_data(db_dict, data)
                        if checked == matched:
                            total_checked += checked
                            total_matched += matched
                            flag = 1
                            break
                except KeyError:
                    total_checked += 1
            if flag == 0:
                self.mismatch[user_id].append(db_dict)
                total_checked += len(db_dict)
        return total_checked, total_matched

    def match_list(self, user_id, db_list, es_list, key=None):
        total_checked = 0
        total_matched = 0
        db_list = db_list or []
        es_list = es_list or []
        if key is None:
            for data in set(db_list + es_list):
                total_checked += 1
                if data in es_list and data in db_list:
                    total_matched += 1
                else:
                    self.mismatch[user_id].append(data)
            return total_checked, total_matched
        else:
            return self.match_list_of_dict_by_key(user_id, db_list, es_list, key)

    def match_profile(self, user_id, db_profiles, es_profiles):
        total_checked = 0
        total_matched = 0
        ignored_params = ["previous_employments", "preferred_employment_types"]
        list_of_dict_params = {
            "skills": "uuid",
            "preferred_locations": "uuid",
            "preferred_countries": "uuid",
            "preferred_industries": "uuid",
            "preferred_job_types": "uuid",
            "it_skills": "uuid",
            "projects": "title",
            "educations": "passing_year",
            "courses_and_certifications": "name",
            "site_contexts_visibility": None,
            "downloaded_by": None,
            "viewed_by": None,
            "emailed_by": None,
            "sms_sent_by": None,
            "follows": None,
            "comments": None,
        }
        db_profiles = db_profiles or []
        es_profiles = es_profiles or []
        for db_profile in db_profiles:
            es_profile = None
            for profile in es_profiles:
                if db_profile["profile_id"] == profile["profile_id"]:
                    es_profile = profile
                    break
            if es_profile is None:
                mismatch_data = {
                    "user_id": user_id,
                    "Database data": db_profile,
                    "ES data": None,
                }
                self.save_error("profile", len(db_profile), 0, mismatch_data)
                total_checked += len(db_profile)
                continue
            for params in db_profile:
                if params in ignored_params:
                    continue
                if params not in es_profile:
                    total_checked += 1
                    self.mismatch[user_id].append(db_profile[params])
                    mismatch_data = {
                        "user_id": user_id,
                        "Database data": db_profile[params],
                        "ES data": None,
                    }
                    self.save_error(params, 1, 0, mismatch_data)
                    continue
                if params in ["preferred_roles", "preferred_roles_unnested"]:
                    checked, matched = self.match_preferred_roles(
                        user_id=user_id,
                        db_list=db_profile[params],
                        es_list=es_profile[params],
                    )
                    total_checked += checked
                    total_matched += matched
                    mismatch_data = {
                        "user_id": user_id,
                        "Database data": db_profile[params],
                        "ES data": es_profile[params],
                    }
                    self.save_error(params, checked, matched, mismatch_data)
                elif params in list_of_dict_params.keys():
                    checked, matched = self.match_list(
                        user_id=user_id,
                        db_list=db_profile[params],
                        es_list=es_profile[params],
                        key=list_of_dict_params[params],
                    )
                    total_checked += checked
                    total_matched += matched
                    mismatch_data = {
                        "user_id": user_id,
                        "Database data": db_profile[params],
                        "ES data": es_profile[params],
                    }
                    self.save_error(params, checked, matched, mismatch_data)
                else:
                    checked, matched = match_data(
                        db_profile[params], es_profile[params]
                    )
                    total_checked += checked
                    total_matched += matched
                    mismatch_data = {
                        "user_id": user_id,
                        "Database data": db_profile[params],
                        "ES data": es_profile[params],
                    }
                    self.save_error(params, checked, matched, mismatch_data)

        return total_checked, total_matched

    def match_users_data(self, es_user_data, db_user_data):
        total_checked = 0
        total_matched = 0
        list_params = [
            "profiles",
            "sms_sent_by",
            "downloaded_by",
            "viewed_by",
            "emailed_by",
            "follows",
            "social_follow_notifications"
        ]
        users_not_found = []
        es_user_data = es_user_data or {}
        db_user_data = db_user_data or {}
        for user_id, user_details in db_user_data.items():
            if user_id not in es_user_data:
                total_checked += len(user_details)
                self.mismatch[user_id].append(user_details)
                users_not_found.append(user_id)
                continue
            for params in user_details:
                if params not in es_user_data[user_id]:
                    total_checked += 1
                    self.mismatch[user_id].append(user_details[params])
                    mismatch_data = {
                        "user_id": user_id,
                        "Database data": user_details[params],
                        "ES data": None,
                    }
                    self.save_error(params, 1, 0, mismatch_data)
                    continue
                if params == "image_url":
                    checked, matched = self.check_image_url(
                        user_id=user_id,
                        db_url=user_details[params],
                        es_url=es_user_data[user_id][params],
                    )
                    total_checked += checked
                    total_matched += matched
                if params in list_params:
                    if params == "profiles":
                        checked, matched = self.match_profile(
                            user_id=user_id,
                            db_profiles=user_details[params],
                            es_profiles=es_user_data[user_id][params],
                        )
                        total_checked += checked
                        total_matched += matched
                    else:
                        checked, matched = self.match_list(
                            user_id=user_id,
                            db_list=user_details[params],
                            es_list=es_user_data[user_id][params],
                        )
                        mismatch_data = {
                            "user_id": user_id,
                            "Database data": user_details[params],
                            "ES data": es_user_data[user_id][params],
                        }
                        self.save_error(params, checked, matched, mismatch_data)
                        total_checked += checked
                        total_matched += matched
                else:
                    if params in es_user_data[user_id].keys():
                        checked, matched = match_data(
                            user_details[params], es_user_data[user_id][params]
                        )
                        if checked != matched:
                            self.mismatch[user_id].append(
                                [user_details[params], es_user_data[user_id][params]]
                            )
                        total_checked += checked
                        total_matched += matched
                        mismatch_data = {
                            "user_id": user_id,
                            "Database data": user_details[params],
                            "ES data": es_user_data[user_id][params],
                        }
                        self.save_error(params, checked, matched, mismatch_data)
                    else:
                        total_checked += 1  # len(user_details[params] or [])
                        mismatch_data = {
                            "user_id": user_id,
                            "Database data": user_details[params],
                            "ES data": None,
                        }
                        self.save_error(params, 1, 0, mismatch_data)
        print("Total Users Fetched from db : %d" % len(db_user_data))
        print("Total Users not found in elastic search : %d" % len(users_not_found))
        print(users_not_found)
        return total_checked, total_matched

    def validate_es_index(self):
        self.mismatch.clear()
        self.get_user_randomly()
        self.get_users_missing(self.user_ids, self.users_dict)
        es_user_data, db_user_data = self.get_user_data()
        total_checked, total_matched = self.match_users_data(es_user_data, db_user_data)
        print("Total Fields Checked : ", str(total_checked))
        print("Total Fields Matched : ", str(total_matched))
        print(
            "Percentage of Error in indexing : ",
            str(100 - (total_matched / total_checked) * 100),
        )
        with open("index_mismatch.txt", "w") as f:
            print(self.mismatch, file=f)
        for key in self.checked:
            if self.checked[key] != self.matched[key]:
                print("\n\n")
                print(
                    "key : %s,  Total Checked: %s,  Total Matched: %s"
                    % (key, self.checked[key], self.matched[key])
                )
                for i in range(min(20, len(self.mismatch[key]))):
                    print(self.mismatch[key][i])

        print("Sample Errors : ")

    def get_users_missing(self, user_ids, users_dict):
        print("number of users : " + str(len(user_ids)))
        kiwi_id_falcon_id = {}
        for user in users_dict:
            kiwi_id_falcon_id[user["id"]] = user["kiwi_user_id"]

        user_ids_str = [str(user) for user in user_ids]
        AWS_ES_END_POINT = "https://search-monster-elastic-qa-egy3hcfr6zaqioo2h5zbeldfqy.ap-south-1.es.amazonaws.com/"
        es_conn = ESUtils.esConn()
        query = {"query": {"ids": {"values": user_ids_str}}}
        es_data = es_conn.search(
            index=self.index_name, body=query, size=len(user_ids_str)
        )
        print("Done")
        es_data = es_data["hits"]["hits"]
        es_user_ids = [int(data["_id"]) for data in es_data]
        print("number of users present in ES :" + str(len(user_ids)))
        missing_kiwi_ids = []
        for falcon_id in user_ids:
            if falcon_id not in es_user_ids:
                missing_kiwi_ids.append(kiwi_id_falcon_id[falcon_id])
        print("number of missing users in es is :" + str(len(missing_kiwi_ids)))
        kiwi_profile_ids_sql_list = (
                "(" + ",".join([str(kiwi_user_id) for kiwi_user_id in missing_kiwi_ids]) + " )"
        )
        sql_query = (
                "select id from bazooka.users where id in " + kiwi_profile_ids_sql_list + " ;"
        )
        # print(sql_query)
        users_dict = DBUtils.fetch_results_in_batch(
            DBUtils().bazooka2_connection(), sql_query, -1, dictionary=True
        )
        present_kiwi_user_ids = [user["id"] for user in users_dict]
        print("Number of present user is : " + str(len(present_kiwi_user_ids)))
        print(
            "Number of Profiles missing are :  "
            + str(len(missing_kiwi_ids) - len(present_kiwi_user_ids))
        )
        print(
            "Missing profiles are : "
            + str(set(missing_kiwi_ids) - set(present_kiwi_user_ids))
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--index-name", default="users", help="index name", required=True
    )
    parser.add_argument(
        "-d", "--days", help="Number of prev days to check", type=int, default=0
    )
    parser.add_argument(
        "--hours", "--hours", help="Number of prev hours to check", type=int, default=0
    )
    parser.add_argument(
        "--mins", "--mins", help="Number of prev mins to check", type=int, default=0
    )
    argv = vars(parser.parse_args())

    index = argv["index_name"]
    no_of_day = argv.get("days")
    no_of_hour = argv.get("hours")
    no_of_min = argv.get("mins")

    validate_index = ValidateIndex(
        index_name=index, days=no_of_day, hours=no_of_hour, mins=no_of_min
    )
    validate_index.validate_es_index()
