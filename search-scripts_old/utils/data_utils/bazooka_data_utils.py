import json
import sys
import argparse
import math
from operator import itemgetter
from collections import defaultdict
from datetime import datetime

# sys.path.append("/home/ashwin.euler/code_restructured_new/utils")
from ..dbutils import DBUtils
from ..utils import Utils


class BazookaDataUtils:

    def appendUserIdsClauseForProfiles(sql_query, col_name, user_ids, user_range):
        if user_ids:
            sql_query += " and %s in %s" % (col_name, user_ids)
        elif user_range:
            sql_query += " and %s >= %d and %s < %d" % (col_name, user_range[0], col_name, user_range[1])
        else:
            raise Exception("Params not set")
        return sql_query

    def getUserLastActiveTime(user_ids, user_details):

        return user_details
        sql_query2 = "select uid,Max(logindate) from bazooka.user_channel_login_map where uid in " + user_ids + " GROUP BY uid ;"
        last_active_time_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka2_connection(), sql_query2, -1)
        last_active_time_dict = {}
        for row in last_active_time_data:
            last_active_time_dict[row[0]] = row[1]

        for row in user_details:
            if row["user_id"] in last_active_time_dict:
                row["last_active_time"] = last_active_time_dict[row["user_id"]]
            else:
                row["last_active_time"] = None

        return user_details
    #iu
    def getUserSeekerServices(user_ids, kiwi_2_user_id):

        sql_query = "select uid, channel_id, subchannel_id, type from bazooka.seeker_brandings where enabled = '1' and uid in %s and expiry > NOW();" % (user_ids)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka2_connection(), sql_query, -1, dictionary=True)
        user_seeker_services = defaultdict(lambda: [])
        for row in db_data:
            if row["uid"] in kiwi_2_user_id:
                user_seeker_services[kiwi_2_user_id[row["uid"]]].append(row["type"] + "%d" % row["subchannel_id"])
        return user_seeker_services

    def getInvitationSentBy(user_ids, kiwi_2_user_id):
        sql_query = "select uid, recruiter_profile_id, subchannel_id from bazooka.social_follow_notification where uid in %s;" % user_ids
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka1_connection(), sql_query, -1, dictionary=True)
        user_social_follow_notifications = defaultdict(lambda: [])
        for row in db_data:
            if row["uid"] in kiwi_2_user_id:
                user_social_follow_notifications[kiwi_2_user_id[row["uid"]]].append(row["recruiter_profile_id"])
        return user_social_follow_notifications


    def getProfileFollows(kiwi_profile_ids, kiwi_2_profile_id):
        sql_query = "select seeker_profile_id as kiwi_profile_id, recruiter_profile_id as follows from bazooka.rec_social_follow where status= '1' and block = '0' and seeker_profile_id in " + kiwi_profile_ids + ";"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka1_connection(), sql_query, -1, dictionary=True)
        profile_follows = defaultdict(lambda: [])
        for row in db_data:
            if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                profile_follows[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(row["follows"])
        return profile_follows

    def getProfileViewedBy(profile_ids, kiwi_2_profile_id):
        sql_query = "select resid as kiwi_profile_id, subuid as viewed_by from bazooka.resumeviewed where tobedeleted ='0' and resid in " + profile_ids + ";"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka3_connection(), sql_query, -1, dictionary=True)
        profile_viewed_by = defaultdict(lambda: [])
        for row in db_data:
            if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                profile_viewed_by[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(row["viewed_by"])
        return profile_viewed_by

    def getProfileDownloadedBy(profile_ids, kiwi_2_profile_id):
        sql_query = "select resid as kiwi_profile_id, subuid from bazooka.download_resume where resid in " + profile_ids + ";"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka3_connection(), sql_query, -1, dictionary=True)
        profile_downloaded_by = defaultdict(lambda: [])
        for row in db_data:
            if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                profile_downloaded_by[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(row["subuid"])
        return profile_downloaded_by

    def getProfileContactedBy(profile_ids, kiwi_2_profile_id):

        sql_query = "select resid as kiwi_profile_id, subuid, cstat from bazooka.resume_contacted where resid in " + profile_ids + ";"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka1_connection(), sql_query, -1, dictionary=True)

        profile_emailed_by = defaultdict(lambda: [])
        profile_sms_sent_by = defaultdict(lambda: [])
        for row in db_data:
            if row["cstat"] == "email":
                if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                    profile_emailed_by[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(row["subuid"])
            elif row["cstat"] == "sms":
                if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                    profile_sms_sent_by[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(row["subuid"])

        return profile_emailed_by, profile_sms_sent_by

    def getProfileComments(profile_ids, kiwi_2_profile_id):

        sql_query = "select resumeid as kiwi_profile_id, subuid, comment, commentdate from bazooka.corp_comments where status='active' and resumeid in " + profile_ids + ";"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.bazooka2_connection(), sql_query, -1, dictionary=True)

        profile_comments = defaultdict(lambda: [])
        for row in db_data:
            if kiwi_2_profile_id.get(row["kiwi_profile_id"]):
                profile_comments[kiwi_2_profile_id[row["kiwi_profile_id"]]].append(
                    {"commented_by": row["subuid"], "comment": row["comment"], "comment_date": row["commentdate"]})

        return profile_comments
