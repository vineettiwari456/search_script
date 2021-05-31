import json
import sys
import argparse
import math
from operator import itemgetter

#sys.path.append("/home/ashwin.euler/code_restructured_new/utils")
from ..dbutils import DBUtils
from ..utils import Utils

class RioDataUtils:

    def getUserDetailsRio(user_uuids, map_dict, uuid_2_user_id):

        if user_uuids:
            sql_query = "select id, CONVERT_TZ(FROM_UNIXTIME(created_at/1000), '+05:30', '+00:00') as created_at, uuid, dob, gender, TRIM(gender_text) as gender_text, marital_status, TRIM(marital_status_text) as marital_status_text, nationality, TRIM(nationality_text) as nationality_text, driving_license, full_name, first_name, avatar from rio.users where uuid in " + user_uuids + ";";
        else:
            raise Exception("No  Params Set")

        users_data = DBUtils.fetch_results_in_batch(DBUtils.rio_connection(), sql_query, -1, dictionary=True)
        if not users_data:
            return {}

        user_ids_as_sql_list = "(" + ",".join([str(row["id"]) for row in users_data]) + ")"

        sql_query = "select user_id, LOWER(email) as email, status from rio.user_emails where primary_email = 1 and user_id in "+ user_ids_as_sql_list + ";";

        email_data = DBUtils.fetch_results_in_batch(DBUtils.rio_connection(), sql_query, -1, dictionary=True)

        email_dict = {}
        for row in email_data :
            email_dict[row["user_id"]] = {
                "id": row["email"],
                "is_verified": row["status"]
            }

        sql_query = "select user_id, number, status, country_code  from rio.user_contact_numbers where primary_contact = 1 and user_id in "+ user_ids_as_sql_list + ";";

        mobile_data = DBUtils.fetch_results_in_batch(DBUtils.rio_connection(), sql_query, -1, dictionary=True)

        mobile_dict = {}
        for row in mobile_data :
            mobile_dict[row["user_id"]] = {
                "country_code": row["country_code"],
                "number": row["number"],
                "is_verified": row["status"]
            }

        gender_dict = map_dict["gender"]
        marital_status_dict = map_dict["marital_status"]
        nationality_dict = map_dict["nationality"]
        user_details = {}
        for row in users_data:
            if uuid_2_user_id.get(row["uuid"]):
                user_details[uuid_2_user_id[row["uuid"]]] = {
                    "dob": row["dob"],
                    "name": row["full_name"] if row["full_name"] else row["first_name"],
                    "creation_time": row["created_at"],
                    "driving_license": row["driving_license"],
                    "image_url": row["avatar"],
                    "has_image": 1 if row["avatar"] and row["avatar"].strip() else 0,
                    "gender": {
                        "uuid": row["gender"],
                        "text": gender_dict[row["gender"]] if gender_dict.get(row["gender"]) else row["gender_text"]
                    },
                    "marital_status": {
                        "uuid": row["marital_status"],
                        "text": marital_status_dict[row["marital_status"]] if marital_status_dict.get(row["marital_status"]) else row["marital_status_text"]
                    },
                    "nationality": {
                        "uuid": row["nationality"],
                        "text": nationality_dict[row["nationality"]]["name"] if nationality_dict.get(row["nationality"]) else row["nationality_text"],
                        "iso_code": nationality_dict[row["nationality"]]["code"] if nationality_dict.get(row["nationality"]) else None
                    },
                    "email": email_dict.get(row["id"]),
                    "mobile_details": mobile_dict.get(row["id"])
                }

        return user_details
