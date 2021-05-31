from collections import defaultdict

from utils.constants import OTHER_LOCATIONS
from utils.data_utils.falcon_data_utils import FalconDataUtils
from utils.dbutils import DBUtils
from utils.utils import Utils


def getProfileandDetails(user_ids, user_range=None, profile_range=None):

    sql_query = "select user_id, id, current_location_uuid, TRIM(current_location_other_text) as current_location_other_text, experience_years , experience_months, title, CONVERT_TZ(FROM_UNIXTIME(created_at/1000), '+05:30', '+00:00') as created_at, site_context, kiwi_profile_id, CONVERT_TZ(FROM_UNIXTIME(profile_updated_at/1000), '+05:30', '+00:00') as updated_at, TRIM(resume_file_path) as resume_file_path, current_salary_absolute_value, current_salary_currency_code, current_salary_mode_uuid,profile_visibility from falcon.user_profiles up where (up.deleted = 0 or up.deleted is NULL) and up.kiwi_profile_id is not NULL and up.enabled = 1 and searchable=1 "

    sql_query = FalconDataUtils.appendUserIdsClauseForProfiles(sql_query, user_ids,
                                                               user_range,
                                                               profile_range)
    print(sql_query)
    db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                             -1, dictionary=True)
    profile_details = defaultdict(lambda: [])
    for row in db_data:
        profile_visibility = None
        if "profile_visibility" in row:
            profile_visibility = row["profile_visibility"]
        print("profile:%s, visi: %s" %(row['id'],row["profile_visibility"]))
        map = {
            "profile_id": row["id"],
            "title": row["title"],
            "creation_time": row["created_at"],
            "update_time": row["updated_at"],
            "resume_file_path": row["resume_file_path"],
            "current_salary_absolute_value": row["current_salary_absolute_value"],
            "current_salary_currency_code": row["current_salary_currency_code"],
            "current_salary_mode_uuid": row["current_salary_mode_uuid"],
            "has_resume": 1 if row["resume_file_path"] else 0,
            "site_context": row["site_context"],
            "kiwi_profile_id": row["kiwi_profile_id"],

        }
        if not profile_visibility is None:
            map["profile_visibility"] = profile_visibility

        profile_details[row["user_id"]].append(map)

        print("------------------%s" % profile_details[row["user_id"]])


    return profile_details

getProfileandDetails(user_ids="(1,2,8000)")