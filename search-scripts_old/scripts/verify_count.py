from datetime import timedelta
import certifi
from elasticsearch import Elasticsearch
from datetime import datetime
import mysql.connector
from contextlib import closing
import json
from os import path

AWS_ES_END_POINT = "https://search-monster-elastic-prod-tyurj6ygcxzrcrqqpfmjkgeroe.ap-south-1.es.amazonaws.com"
PATH_TO_FILES = "./"

CHANNEL_TO_SITE_CONTEXT_MAP = {
    "India": ["rexmonster"],
    "Gulf": ["monstergulf"],
    "SEA": [
        "monstersingapore",
        "monsterphilippines",
        "monsterthailand",
        "monstervietnam",
        "monsterindonesia",
        "monstermalaysia",
        "monsterhongkong"
    ]
}

USER_IDS_BY_CONTEXT_AND_PREFERRED_LOCATIIONS_PROFILE_IDS = "SELECT DISTINCT(user_id) AS user_id FROM user_profiles up WHERE (id IN (%s) OR site_context IN (%s)) AND up.searchable=1 AND up.kiwi_profile_id IS NOT NULL AND (up.deleted = 0 or up.deleted is NULL) AND up.enabled = 1; "

LOCATION_UUIDS_BY_CONTEXT = "SELECT DISTINCT(uuid) AS uuid FROM job_locations WHERE site_context IN (%s);"

PROFILES_BY_PREFERRED_LOCATIONS = "SELECT DISTINCT(a2.profile_id) AS profile_id FROM falcon.user_job_preferences_locations a1 , falcon.user_job_preferences a2 WHERE (a1.deleted = 0 OR a1.deleted IS NULL) AND (a2.deleted = 0 OR a2.deleted IS NULL) AND  a2.id=a1.job_preferences_id AND a1.location_uuid IN (%s);"

USER_IDS_BY_ACTIVE_TIME = "SELECT DISTINCT(user_id) AS user_id FROM user_active_data WHERE active_at >= UNIX_TIMESTAMP(CONVERT_TZ(DATE(NOW() - INTERVAL 180 DAY), '+00:00', '+05:30'))*1000;"

USER_IDS_BY_CREATION_TIME = "SELECT DISTINCT(user_id) AS user_id FROM user_profiles WHERE (deleted = 0 or deleted is NULL) AND enabled = 1 AND searchable=1 AND kiwi_profile_id IS NOT NULL AND created_at >= UNIX_TIMESTAMP(CONVERT_TZ(DATE(NOW() - INTERVAL 180 DAY), '+00:00', '+05:30'))*1000;"


def get_falcon_db():
    while 1:
        try:
            db = mysql.connector.connect(host="10.216.247.108", user="sandeep.euler", passwd="sandeep123", db="falcon")
            break
        except Exception as e:
            print("Error in get_falcon_db: %s", e)
    return db


def fetch_results_in_batch(query, batch_size, dictionary=False):
    rows = []
    while 1:
        try:
            connection = get_falcon_db()
            with closing(connection.cursor(buffered=True, dictionary=dictionary)) as cursor:
                print("Executing Query at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
                cursor.execute(query)
                print("Query executed... Fetching results at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
                if batch_size == -1:
                    rows = cursor.fetchall()
                else:
                    batch = cursor.fetchmany(batch_size)
                    while batch:
                        rows += batch
                        batch = cursor.fetchmany(batch_size)
            connection.close()
            break
        except KeyboardInterrupt:
            if bool(int(input("\nWISH TO STOP: 1, NO: 0\n"))):
                raise KeyboardInterrupt
        except Exception as e:
            print("Error in fetch_results_in_batch: %s" % e)
    return rows


def read_file(file_name):
    print("Reading file: %s" % file_name)
    if path.exists(PATH_TO_FILES + file_name):
        with open(PATH_TO_FILES + file_name) as f:
            file_dict = json.load(f)
            if file_dict["count"] == len(file_dict["ids"]):
                return set(file_dict["ids"])
            else:
                print("Count issue in written file")
    else:
        print("File not available.")
    return set()


def get_active_or_created_user_ids(file_name, query, user_type):
    user_ids = read_file(file_name)
    if not user_ids:
        print("Fetching user ids by %s time at: %s" % (user_type, datetime.now().strftime("%b %d %Y %H:%M:%S")))
        rows = fetch_results_in_batch(query, -1, dictionary=True)
        print("Fetched user ids by %s time at: %s" % (user_type, datetime.now().strftime("%b %d %Y %H:%M:%S")))
        user_ids = set([row["user_id"] for row in rows])

        with open(PATH_TO_FILES + file_name, "w+") as f:
            json.dump({"count": len(user_ids), "ids": list(user_ids)}, f)
    print("Count: %d\n" % len(user_ids))
    return user_ids


def execute_query_in_batches(query_string, key, list_values, batch_size, site_contexts=None):
    all_ids = []
    for i in range(0, len(list_values), batch_size):
        print("Batch Range: [%d, %d)" % (i, min(i + batch_size, len(list_values))))
        batch = list_values[i:i + batch_size]
        if len(batch) != min(batch_size, len(list_values) - i):
            print("Error in creating batch")

        if site_contexts is None:
            query = query_string % ", ".join(["'%s'" % str(value) for value in batch])
        else:
            query = query_string % (", ".join([str(value) for value in batch]), ", ".join(["'%s'" % context for context in site_contexts]))
        rows = fetch_results_in_batch(query, -1, dictionary=True)
        all_ids += [row[key] for row in rows]
    return set(all_ids)


def user_count(channel_name, user_ids_by_active_time, user_ids_by_creation_time):
    site_contexts = CHANNEL_TO_SITE_CONTEXT_MAP[channel_name]
    file_name = "%s_profiles_preferred_locations_%s.json" % (channel_name, str(datetime.now().date()))
    profile_ids = list(read_file(file_name))
    if not profile_ids:
        location_uuid_query = LOCATION_UUIDS_BY_CONTEXT % ", ".join(["'%s'" % context for context in site_contexts])
        print("Fetching location uuids at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        rows = fetch_results_in_batch(location_uuid_query, -1, dictionary=True)
        print("Fetched location uuids at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        location_uuids = [row["uuid"] for row in rows]
        print("Count: %d\n" % len(location_uuids))

        print("Fetching profile_ids by preferred locations at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        profile_ids = list(execute_query_in_batches(PROFILES_BY_PREFERRED_LOCATIONS, "profile_id", location_uuids, 3000))
        print("Fetched profile_ids by preferred locations at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        with open(PATH_TO_FILES + file_name, "w+") as f:
            json.dump({"count": len(profile_ids), "ids": profile_ids}, f)
    print("Count: %d\n" % len(profile_ids))

    file_name = "%s_users_visibility_%s.json" % (channel_name, str(datetime.now().date()))
    user_ids = read_file(file_name)
    if not user_ids:
        print("Fetching user_ids by profile_ids and context at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        user_ids = execute_query_in_batches(USER_IDS_BY_CONTEXT_AND_PREFERRED_LOCATIIONS_PROFILE_IDS, "user_id", profile_ids, 50000000, site_contexts)
        print("Fetched user_ids by profile_ids and context at: %s" % datetime.now().strftime("%b %d %Y %H:%M:%S"))
        with open(PATH_TO_FILES + file_name, "w+") as f:
            json.dump({"count": len(user_ids), "ids": list(user_ids)}, f)
    print("Count: %d\n" % len(user_ids))

    return user_ids.intersection(user_ids_by_active_time), user_ids.intersection(user_ids_by_creation_time)


def main():
    count_dict = {
        "India": {},
        "Gulf": {},
        "SEA": {}
    }

    try:
        user_ids_by_active_time = get_active_or_created_user_ids("active_user_ids_%s.json" % str(datetime.now().date()), USER_IDS_BY_ACTIVE_TIME, "active")
        user_ids_by_creation_time = get_active_or_created_user_ids("created_user_ids_%s.json" % str(datetime.now().date()), USER_IDS_BY_CREATION_TIME, "created")
        channels = ["India", "Gulf", "SEA"]

        for channel in channels:
            print("________________________________________________________________")
            print("Channel %s:" % channel)
            print("Starting count at: %s \n" % datetime.now().strftime("%b %d %Y %H:%M:%S"))

            filename_active = channel + "_active_ids.json"
            filename_created = channel + "_created_ids.json"
            active_ids = read_file(filename_active)
            created_ids = read_file(filename_created)

            if not (active_ids and created_ids):
                active_ids, created_ids = user_count(channel, user_ids_by_active_time, user_ids_by_creation_time)
                with open(PATH_TO_FILES + filename_active, "w+") as f:
                    json.dump({"count": len(active_ids), "ids": list(active_ids)}, f)
                with open(PATH_TO_FILES + filename_created, "w+") as f:
                    json.dump({"count": len(created_ids), "ids": list(created_ids)}, f)

            count_dict[channel]["active"] = len(active_ids)
            count_dict[channel]["created"] = len(created_ids)

    except KeyboardInterrupt:
        pass
    print("\nFinal Counts:")
    print(json.dumps(count_dict, indent=2))


if __name__ == '__main__':
    main()
