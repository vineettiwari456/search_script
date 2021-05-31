from collections import defaultdict
from datetime import datetime

from ..dbutils import DBUtils
from ..utils import Utils
from ..constants import OTHER_LOCATIONS, OTHER_COLLEGE, OTHER_HIGHEST_QUALIFICATION, \
    OTHER_SPECIALIZATIONS


class FalconDataUtils:

    def getMaxUserId():
        sql_query = "select max(id) from users;"
        result = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                -1)
        return result[0][0]

    def getMaxProfileId():
        sql_query = "select max(id) from user_profiles;"
        result = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                -1)
        return result[0][0]

    def getUserIds(user_count):
        sql_query = "select id from  falcon.users where (deleted = 0 or deleted is NULL) and kiwi_user_id is not NULL limit " + str(
            user_count) + " ;"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1)
        return [str(row[0]) for row in db_data]

    def getKiwiUserIds(user_range, as_sql_list=False):
        sql_query = "select kiwi_user_id from users where id >= %d and id <= %d and kiwi_user_id is not null" % (
            user_range[0], user_range[1])
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1)
        return [str(row[0]) for row in db_data]

    def getKiwiProfileIds(user_range):
        sql_query = "select kiwi_profile_id from user_profiles where user_id >= %d and user_id <= %d and kiwi_profile_id is not null" % (
            user_range[0], user_range[1])
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1)
        return [str(row[0]) for row in db_data]

    def appendUserIdsClauseForProfiles(sql_query, user_ids, user_range, profile_range,
                                       with_semicolon=True):
        if user_ids:
            sql_query += " and user_id in %s" % user_ids
        elif user_range:
            sql_query += " and user_id >= %d and user_id < %d" % (
                user_range[0], user_range[1])
        elif profile_range:
            sql_query += " and id >= %d and id < %d" % (
                profile_range[0], profile_range[1])
        else:
            raise Exception("Params not set")
        if with_semicolon:
            sql_query += ";"
        return sql_query

    def appendProfileIdsClause(sql_query, profile_ids=None, user_range=None,
                               profile_range=None, with_semicolon=True, with_and=True):
        if with_and:
            sql_query = sql_query + " and "

        if profile_ids:
            sql_query += " profile_id in %s" % profile_ids
        elif user_range:
            sql_query += " profile_id in (select id from user_profiles where user_id >= %d and user_id < %d)" % (
                user_range[0], user_range[1])
        elif profile_range:
            sql_query += " profile_id >= %d and profile_id < %d " % (
                profile_range[0], profile_range[1])
        else:
            raise Exception("Params not set")
        if with_semicolon:
            sql_query += ";"
        return sql_query

    def getUsersandDetails(user_ids, map_dict, user_range=None):

        if user_ids:
            sql_query = "select id, uuid, kiwi_user_id, category_uuid, TRIM(category) as category, differently_abled, visibility, current_location_work_visa, status, spl_source, covid_layoffed from  falcon.users  where (deleted = 0 or deleted is NULL)  and kiwi_user_id is not NULL and id in %s;" % user_ids
        elif user_range:
            sql_query = "select id, uuid, kiwi_user_id, category_uuid, TRIM(category) as category, differently_abled, visibility, current_location_work_visa, status, spl_source, covid_layoffed from  falcon.users  where (deleted = 0 or deleted is NULL)  and kiwi_user_id is not NULL and id >= %d and id < %d;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("No param set")
        # print(sql_query)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)

        user_details = {}
        category_dict = map_dict["category"]
        for row in db_data:
            user_details[row["id"]] = {
                "user_id": row["id"],
                "kiwi_user_id": row["kiwi_user_id"],
                "uuid": row["uuid"],
                "category": {
                    "uuid": row["category_uuid"],
                    "text": category_dict[row["category_uuid"]] if category_dict.get(
                        row["category_uuid"]) else row[
                        "category"]
                },
                "is_differently_abled": row["differently_abled"] if row[
                                                                        "differently_abled"] is not None else 0,
                "is_confidential": (1 - row["visibility"]) if row[
                                                                  "visibility"] is not None else 0,
                "current_location_work_visa": row["current_location_work_visa"],
                "is_active": row["status"],
                "source": "SEO" if row["spl_source"] is None else "SPL",
                "covid_layoffed": row["covid_layoffed"]
            }

        return user_details

    def getUserLastActiveTime(user_ids, map_dict, user_range=None):
        if user_ids:
            sql_query = "select user_id, CONVERT_TZ(FROM_UNIXTIME(max(active_at)/1000), '+05:30', '+00:00') as active_at from user_active_data where user_id in %s group by user_id;" % user_ids
        elif user_range:
            sql_query = "select user_id, CONVERT_TZ(FROM_UNIXTIME(max(active_at)/1000), '+05:30', '+00:00') as active_at from user_active_data where user_id >= %d AND user_id < %d group by user_id;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("Non param set")
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        user_last_active_time = {}
        for row in db_data:
            user_last_active_time[row["user_id"]] = row["active_at"]

        return user_last_active_time

    def getProfileandDetails(user_ids, map_dict, user_range=None, profile_range=None):

        # sql_query = "select user_id, id, current_location_uuid, TRIM(current_location_other_text) as current_location_other_text, experience_years , experience_months, title, CONVERT_TZ(FROM_UNIXTIME(created_at/1000), '+05:30', '+00:00') as created_at, site_context, kiwi_profile_id, CONVERT_TZ(FROM_UNIXTIME(profile_updated_at/1000), '+05:30', '+00:00') as updated_at, TRIM(resume_file_path) as resume_file_path, current_salary_absolute_value, current_salary_currency_code, current_salary_mode_uuid,profile_visibility from falcon.user_profiles up where (up.deleted = 0 or up.deleted is NULL) and up.kiwi_profile_id is not NULL and up.enabled = 1 and searchable=1 "
        sql_query = "select up.user_id, up.id, up.current_location_uuid, TRIM(up.current_location_other_text) as " \
                    "current_location_other_text, up.experience_years , up.experience_months, up.title, " \
                    "CONVERT_TZ(FROM_UNIXTIME(up.created_at/1000), '+05:30', '+00:00') as created_at, up.site_context," \
                    " up.kiwi_profile_id, CONVERT_TZ(FROM_UNIXTIME(up.profile_updated_at/1000), '+05:30', '+00:00')" \
                    " as updated_at, TRIM(up.resume_file_path) as resume_file_path, up.current_salary_absolute_value, " \
                    "up.current_salary_currency_code, up.current_salary_mode_uuid,up.profile_visibility " \
                    " ,u.experience_level from falcon.user_profiles up " \
                    " inner join users u on (up.user_id=u.id)" \
                    " where (up.deleted = 0 or up.deleted is NULL) and " \
                    " up.kiwi_profile_id is not NULL and up.enabled = 1 and searchable=1 "

        sql_query = FalconDataUtils.appendUserIdsClauseForProfiles(sql_query, user_ids,
                                                                   user_range,
                                                                   profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        channel_site_dict = map_dict["channel_site"]
        sub_channel_site_dict = map_dict["sub_channel_site"]
        country_mapping_dict = map_dict["country_mapping"]
        location_dict = map_dict["locations"]
        profile_details = defaultdict(lambda: [])
        for row in db_data:
            profile_visibility = None
            if "profile_visibility" in row:
                profile_visibility = row["profile_visibility"]
            current_country_uuid = country_mapping_dict.get(row["current_location_uuid"])
            experience_years = row["experience_years"]
            experience_months = row["experience_months"]
            if experience_years is None and experience_months is None:
                experience_level = row['experience_level']
                if experience_level:
                    if experience_level.upper() == 'FRESHER':
                        experience_years = 0
                        experience_months = 0

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
                "current_location": {
                    "uuid": row["current_location_uuid"],
                    "text": location_dict[
                        row["current_location_uuid"]] if location_dict.get(
                        row["current_location_uuid"]) and row[
                                                             "current_location_uuid"] not in OTHER_LOCATIONS else
                    row["current_location_other_text"] if row[
                        "current_location_other_text"] else location_dict.get(
                        row["current_location_uuid"])
                },
                "current_country": {
                    "uuid": current_country_uuid,
                    "text": location_dict.get(current_country_uuid)
                },
                # "experience": None if row["experience_years"] is None and row[
                #     "experience_months"] is None else Utils.joinAtDecimal(
                #     row["experience_years"] if row["experience_years"] else 0,
                #     row["experience_months"] if row["experience_months"] else 0),
                "experience": None if experience_years is None and experience_months is None else Utils.joinAtDecimal(
                    experience_years if experience_years is not None else 0,
                    experience_months if experience_months is not None else 0),

                "channel_id": channel_site_dict[
                    row["site_context"]] if channel_site_dict.get(
                    row["site_context"]) else None,
                "sub_channel_id": sub_channel_site_dict[
                    row["site_context"]] if sub_channel_site_dict.get(
                    row["site_context"]) else None,
            }

            if not profile_visibility is None:
                map["profile_visibility"] = profile_visibility

            profile_details[row["user_id"]].append(map)

        return profile_details

    def getUserWorkPermits(user_ids, map_dict, user_range=None):

        if user_ids:
            sql_query = "select user_id, country, visa_type_uuid from user_work_permit_countries where (deleted = 0 or deleted is NULL) and  user_id in " + user_ids + ";"
        elif user_range:
            sql_query = "select user_id, country, visa_type_uuid from user_work_permit_countries where (deleted = 0 or deleted is NULL) and  user_id >= %d and user_id < %d;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("no Param set")

        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        country_dict = map_dict["country"]
        visa_type_dict = map_dict["visa_type"]

        user_work_auths = defaultdict(lambda: [])
        for row in db_data:
            user_work_auths[row["user_id"]].append({
                "visa_type": {
                    "uuid": row["visa_type_uuid"],
                    "text": visa_type_dict.get(row["visa_type_uuid"])
                },
                "country": {
                    "uuid": row["country"],
                    "text": country_dict[row["country"]]["name"] if country_dict.get(
                        row["country"]) else None,
                    "iso_code": country_dict[row["country"]]["code"] if country_dict.get(
                        row["country"]) else None,
                }
            })
        return user_work_auths

    def getUserPR(user_ids, map_dict, user_range=None):

        if user_ids:
            sql_query = "select user_id, country_uuid, country_text from user_resident_countries where (deleted = 0 or deleted is NULL) and  user_id in " + user_ids + ";"
        elif user_range:
            sql_query = "select user_id, country_uuid, country_text from user_resident_countries where (deleted = 0 or deleted is NULL) and  user_id >= %d and user_id < %d;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("no Param set")

        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        country_dict = map_dict["country"]

        user_prs = defaultdict(lambda: [])
        for row in db_data:
            user_prs[row["user_id"]].append({
                "uuid": row["country_uuid"],
                "text": country_dict[row["country_uuid"]]["name"] if country_dict.get(
                    row["country_uuid"]) else row["country_text"],
                "iso_code": country_dict[row["country_uuid"]]["code"] if country_dict.get(
                    row["country_uuid"]) else None,
            })
        return user_prs

    def getHideEmployers(user_ids, map_dict, user_range=None):

        if user_ids:
            sql_query = "select user_id, company_uuid, TRIM(company_text) as company_text from falcon.user_blocked_companies where (deleted = 0 or deleted is NULL) and blocked = 1 and user_id in " + user_ids + ";"
        elif user_range:
            sql_query = "select user_id, company_uuid, TRIM(company_text) as company_text from falcon.user_blocked_companies where (deleted = 0 or deleted is NULL) and blocked = 1 and user_id >= %d and user_id < %d;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("No param set")

        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        company_dict = map_dict["company"]
        user_hide_employers = defaultdict(lambda: [])
        for row in db_data:
            user_hide_employers[row["user_id"]].append({
                "uuid": row["company_uuid"],
                "text": company_dict[row["company_uuid"]] if company_dict.get(
                    row["company_uuid"]) else row[
                    "company_text"]
            })
        return user_hide_employers

    def getProfileLanguages(profile_ids, map_dict, user_range=None, profile_range=None):

        sql_query = "select profile_id, language_uuid, TRIM(language_text) as language_text from falcon.user_profile_languages  where (deleted = 0 or deleted is NULL) "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)

        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        language_dict = map_dict["languages"]
        profile_languages = defaultdict(lambda: [])
        for row in db_data:
            profile_languages[row["profile_id"]].append({
                "uuid": row["language_uuid"],
                "text": language_dict[row["language_uuid"]] if language_dict.get(
                    row["language_uuid"]) else row[
                    "language_text"]
            })
        return profile_languages

    def getProfilePreferredLocationsAndCountries(profile_ids, map_dict, user_range=None,
                                                 profile_range=None):

        sql_query = "select a2.profile_id as profile_id, a1.location_uuid, TRIM(a1.location_text) as location_text from falcon.user_job_preferences_locations a1 , falcon.user_job_preferences a2 where (a1.deleted = 0 or a1.deleted is NULL) and (a2.deleted = 0 or a2.deleted is NULL) and  a2.id=a1.job_preferences_id "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        location_dict = map_dict["locations"]
        country_mapping_dict = map_dict["country_mapping"]
        profile_preferred_locations = defaultdict(lambda: [])
        profile_preferred_countries = defaultdict(lambda: [])
        for row in db_data:
            profile_preferred_locations[row["profile_id"]].append({
                "uuid": row["location_uuid"],
                "text": location_dict[row["location_uuid"]] if location_dict.get(
                    row["location_uuid"]) else row[
                    "location_text"]
            })
        for profile_id, preferred_locations in profile_preferred_locations.items():
            country_uuids = list(set(filter(lambda x: x, [
                country_mapping_dict.get(preferred_location["uuid"]) for
                preferred_location in preferred_locations])))
            for country_uuid in country_uuids:
                profile_preferred_countries[profile_id].append({
                    "uuid": country_uuid,
                    "text": location_dict.get(country_uuid)
                })

        return profile_preferred_locations, profile_preferred_countries

    def getProfilePreferredIndustries(profile_ids, map_dict, user_range=None,
                                      profile_range=None):

        sql_query = "select a2.profile_id as profile_id, a1.industry_uuid, TRIM(a1.industry_text) as industry_text from falcon.user_job_preferences_industries a1 , falcon.user_job_preferences a2 where (a1.deleted = 0 or a1.deleted is NULL) and (a2.deleted = 0 or a2.deleted is NULL) and  a2.id=a1.job_preferences_id"
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        industry_dict = map_dict["industries"]
        profile_preferred_industries = defaultdict(lambda: [])
        for row in db_data:
            profile_preferred_industries[row["profile_id"]].append({
                "uuid": row["industry_uuid"],
                "text": industry_dict[row["industry_uuid"]] if industry_dict.get(
                    row["industry_uuid"]) else row[
                    "industry_text"]
            })
        return profile_preferred_industries

    def getProfilePreferredRoles(profile_ids, map_dict, user_range=None,
                                 profile_range=None):

        sql_query = "select a2.profile_id as profile_id, a1.role_uuid, TRIM(a1.role_text) as role_text from falcon.user_job_preferences_roles a1, falcon.user_job_preferences a2 where (a1.deleted = 0 or a1.deleted is NULL) and (a2.deleted = 0 or a2.deleted is NULL) and a2.id=a1.job_preferences_id "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        role_dict = map_dict["roles"]
        _profile_preferred_roles = defaultdict(lambda: defaultdict(lambda: []))
        function_role_dict = map_dict["function_and_role"]

        for row in db_data:
            function_uuid = function_role_dict[
                row["role_uuid"]] if function_role_dict.get(row["role_uuid"]) else "null"
            _profile_preferred_roles[row["profile_id"]][function_uuid].append({
                "uuid": row["role_uuid"],
                "text": role_dict[row["role_uuid"]] if role_dict.get(
                    row["role_uuid"]) else row["role_text"]
            })
        profile_preferred_roles = defaultdict(lambda: [])
        for profile_id, function_roles in _profile_preferred_roles.items():
            for function_uuid, roles in function_roles.items():
                profile_preferred_roles[profile_id].append({
                    "function": {
                        "uuid": None if function_uuid == "null" else function_uuid,
                        "text": role_dict[
                            function_uuid] if function_uuid != "null" else None
                    },
                    "roles": roles
                })
        return profile_preferred_roles

    def getProfilePreferredJobEmploymentType(profile_ids, map_dict, user_range=None,
                                             profile_range=None):

        sql_query = "select a2.profile_id as profile_id, a2.job_type_uuid, a2.employment_type_uuid from  falcon.user_job_preferences a2  where (a2.deleted = 0 or a2.deleted is NULL) "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        job_type_dict = map_dict["job_type"]
        employment_type_dict = map_dict["employment_type"]
        profile_preferred_job_types = defaultdict(lambda: [])
        profile_preferred_employment_types = defaultdict(lambda: [])
        for row in db_data:
            profile_preferred_job_types[row["profile_id"]].append({
                "uuid": row["job_type_uuid"],
                "text": job_type_dict[row["job_type_uuid"]] if job_type_dict.get(
                    row["job_type_uuid"]) else None
            })
            profile_preferred_employment_types[row["profile_id"]].append({
                "uuid": row["employment_type_uuid"],
                "text": employment_type_dict[
                    row["employment_type_uuid"]] if employment_type_dict.get(
                    row["employment_type_uuid"]) else None
            })
        return profile_preferred_job_types, profile_preferred_employment_types

    def getProfileSkills(profile_ids, map_dict, user_range=None, profile_range=None):

        sql_query = "select profile_id, skill_uuid, TRIM(skill_text) as skill_text, last_used, experience_years, experience_months, version, category from falcon.user_profile_skills  where (deleted = 0 or deleted is NULL) "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        skill_dict = map_dict["skills"]
        skill_it_dict = map_dict["it_skill"]
        profile_skills = defaultdict(lambda: [])
        profile_it_skills = defaultdict(lambda: [])
        for row in db_data:
            profile_skills[row["profile_id"]].append({
                "uuid": row["skill_uuid"],
                "text": skill_dict[row["skill_uuid"]] if skill_dict.get(
                    row["skill_uuid"]) else row["skill_text"]
            })
            if row["skill_uuid"] in skill_it_dict or row["category"] == "IT":
                profile_it_skills[row["profile_id"]].append({
                    "uuid": row["skill_uuid"],
                    "text": skill_dict[row["skill_uuid"]] if skill_dict.get(
                        row["skill_uuid"]) else row["skill_text"],
                    "version": row["version"],
                    "last_used": row["last_used"],
                    "experience": None if row["experience_years"] is None and row[
                        "experience_months"] is None else Utils.joinAtDecimal(
                        row["experience_years"] if row["experience_years"] else 0,
                        row["experience_months"] if row["experience_months"] else 0),
                })

        return profile_skills, profile_it_skills

    def getProfileCourseandCertifications(profile_ids, user_range=None,
                                          profile_range=None):

        sql_query = "select profile_id, name, issuer, lifetime_validity from falcon.user_profile_certifications where (deleted = 0 or deleted is NULL) "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        profile_courses_and_certifications = defaultdict(lambda: [])

        for row in db_data:
            profile_courses_and_certifications[row["profile_id"]].append({
                "name": row["name"],
                "issuer": row["issuer"],
                "lifetime_validity": row["lifetime_validity"]
            })

        return profile_courses_and_certifications

    def getProfileProjects(profile_ids, user_range=None, profile_range=None):

        sql_query = "select profile_id, title, description, start_date, end_date from falcon.user_profile_projects  where (deleted = 0 or deleted is NULL) "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        profile_projects = defaultdict(lambda: [])

        for row in db_data:
            profile_projects[row["profile_id"]].append({
                "title": row["title"],
                "description": row["description"],
                "start_date": row["start_date"],
                "end_date": row["end_date"]
            })

        return profile_projects

    def getCurrentEmploymentDetails(profile_ids, map_dict, user_range=None,
                                    profile_range=None):
        sql_query = "select profile_id ,notice_period_days as notice_period, designation_uuid  , designation_text, salary_absolute_value, salary_currency_code, salary_mode, company_uuid, company_text, start_date from falcon.user_profile_employments  where  (deleted = 0 or deleted is NULL) and is_current=1 "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1)
        designation_dict = map_dict["designation"]
        company_dict = map_dict["company"]
        conversion_to_usd_dict = map_dict["currency_conversion"]
        salary_mode_dict = map_dict["salary_mode"]
        profile_current_employment_list = []

        for row in db_data:
            pdict = {}
            adict = {}
            pdict["profile_id"] = row[0]
            adict["notice_period"] = row[1]

            empdict_1 = {}
            empdict_1["uuid"] = row[2]
            if row[3] is not None:
                empdict_1["text"] = row[3].strip()
            else:
                empdict_1["text"] = None

            if str(empdict_1["uuid"]) in designation_dict:
                empdict_1["text"] = designation_dict[str(empdict_1["uuid"])]
            adict["designation"] = empdict_1

            salary_absolute_value = row[4]
            currency_code = row[5]
            salary_mode = row[6]
            if str(salary_mode) in salary_mode_dict:
                salary_mode = salary_mode_dict[str(salary_mode)]

            if str(currency_code) in conversion_to_usd_dict:
                conversion_rate = conversion_to_usd_dict[str(currency_code)]
            else:
                conversion_rate = 1.0

            if salary_absolute_value != None:
                if salary_mode == "Monthly":
                    adict["ctc"] = ((float(salary_absolute_value)) * 12.0) * (
                        conversion_rate)
                else:
                    adict["ctc"] = (float(salary_absolute_value)) * (conversion_rate)
            else:
                adict["ctc"] = None
            adict["currency_code"] = currency_code

            empdict_2 = {}
            empdict_2["uuid"] = row[7]
            if row[8] is not None:
                empdict_2["text"] = row[8].strip()
            else:
                empdict_2["text"] = None

            if str(empdict_2["uuid"]) in company_dict:
                empdict_2["text"] = company_dict[str(empdict_2["uuid"])]
            adict["employer"] = empdict_2
            adict["start_date"] = row[9]
            pdict["current_employment"] = adict
            profile_current_employment_list.append(pdict)

        profile_current_employment_list = Utils.combineColumns(
            profile_current_employment_list, "profile_id",
            "current_employment")
        profile_current_employment = {}
        for row in profile_current_employment_list:
            profile_current_employment[row["profile_id"]] = row["current_employment"]

        return profile_current_employment

    def getEmploymentDetails(profile_ids, map_dict, user_range=None, profile_range=None,
                             profile_2_site_context=None):
        sql_query = "select profile_id, notice_period_days as notice_period, designation_uuid, TRIM(designation_text) as designation_text, salary_absolute_value, salary_currency_code, salary_mode, company_uuid, TRIM(company_text) as company_text, start_date, end_date from falcon.user_profile_employments  where  (deleted = 0 or deleted is NULL)"
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range,
                                                           with_semicolon=False)
        sql_query += " ORDER BY CASE WHEN end_date=\'\' OR end_date IS NULL OR end_date=\'0000-00-00\' THEN 1 else 0 END DESC, end_date DESC, start_date DESC, updated_at DESC"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        designation_dict = map_dict["designation"]
        company_dict = map_dict["company"]
        salary_mode_dict = map_dict["salary_mode"]
        profile_employments = defaultdict(lambda: [])

        for row in db_data:
            final_ctc, final_usd_ctc = Utils.finalCTC(map_dict, row["salary_mode"],
                                                      row["salary_currency_code"],
                                                      row["salary_absolute_value"],
                                                      profile_2_site_context[
                                                          row["profile_id"]])

            profile_employments[row["profile_id"]].append({
                "ctc": row["salary_absolute_value"],
                "final_ctc": final_ctc,
                "usd_ctc": final_usd_ctc,
                "salary_mode": salary_mode_dict.get(row["salary_mode"]),
                "employer": {
                    "uuid": row["company_uuid"],
                    "text": company_dict[row["company_uuid"]] if company_dict.get(
                        row["company_uuid"]) else row[
                        "company_text"]
                },
                "designation": {
                    "uuid": row["designation_uuid"],
                    "text": designation_dict[
                        row["designation_uuid"]] if designation_dict.get(
                        row["designation_uuid"]) else row["designation_text"]
                },
                "currency_code": row["salary_currency_code"],
                "notice_period": row["notice_period"],  # TODO look into notice period
                "start_date": row["start_date"],
                "end_date": row["end_date"]
            })

        profile_previous_employments = {}
        profile_current_employment = {}
        min_datetime = datetime.strptime("1970-01-01", "%Y-%m-%d").date()
        for profile_id, employments in profile_employments.items():
            if employments:
                employments.sort(key=lambda emp: (
                    max(emp["start_date"] or min_datetime, emp["end_date"] or min_datetime),
                    1 if (emp["start_date"] or min_datetime) >= (emp["end_date"] or min_datetime) else 0), reverse=True)
                profile_current_employment[profile_id] = employments[0]
                profile_previous_employments[profile_id] = employments[1:]

        return profile_current_employment, profile_previous_employments

    def getPreviousEmploymentDetails(profile_ids, map_dict, user_range=None,
                                     profile_range=None):

        sql_query = "select profile_id, designation_uuid  , designation_text, salary_absolute_value, salary_currency_code, salary_mode, company_uuid, company_text, start_date, end_date from falcon.user_profile_employments  where  (deleted = 0 or deleted is NULL) and  is_current!=1 "
        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1)
        designation_dict = map_dict["designation"]
        company_dict = map_dict["company"]
        conversion_to_usd_dict = map_dict["currency_conversion"]
        salary_mode_dict = map_dict["salary_mode"]
        profile_previous_employment_list = []

        for row in db_data:
            pdict = {}
            adict = {}
            pdict["profile_id"] = row[0]
            empdict_1 = {}
            empdict_1["uuid"] = row[1]
            if row[2] is not None:
                empdict_1["text"] = row[2].strip()
            else:
                empdict_1["text"] = None

            if str(empdict_1["uuid"]) in designation_dict:
                empdict_1["text"] = designation_dict[str(empdict_1["uuid"])]
            adict["designation"] = empdict_1

            salary_absolute_value = row[3]
            currency_code = row[4]
            salary_mode = row[5]
            if str(salary_mode) in salary_mode_dict:
                salary_mode = salary_mode_dict[str(salary_mode)]

            if str(currency_code) in conversion_to_usd_dict:
                conversion_rate = conversion_to_usd_dict[str(currency_code)]
            else:
                conversion_rate = 1.0

            adict["currency_code"] = currency_code

            empdict_2 = {}
            empdict_2["uuid"] = row[6]
            if row[7] is not None:
                empdict_2["text"] = row[7].strip()
            else:
                empdict_2["text"] = None

            if str(empdict_2["uuid"]) in company_dict:
                empdict_2["text"] = company_dict[str(empdict_2["uuid"])]
            adict["employer"] = empdict_2
            adict["start_date"] = row[8]
            adict["end_date"] = row[9]
            pdict["previous_employments"] = adict
            profile_previous_employment_list.append(pdict)

        profile_previous_employment_list = Utils.combineColumns(
            profile_previous_employment_list, "profile_id",
            "previous_employments")
        profile_previous_employments = {}
        for row in profile_previous_employment_list:
            profile_previous_employments[row["profile_id"]] = row["previous_employments"]

        return profile_previous_employments

    def getEducationDetails(profile_ids, map_dict, user_range=None, profile_range=None):

        sql_query = "select  id, profile_id, highest_qualification_uuid, TRIM(highest_qualification_text) as highest_qualification_text, specialization_uuid, TRIM(specialization_text) as specialization_text, college_uuid, TRIM(college_text) as college_text, year_of_passing  from falcon.user_profile_educations  where ((highest_qualification_uuid IS NOT NULL) or (highest_qualification_text IS NOT NULL) or (specialization_uuid IS NOT NULL) or (specialization_text IS NOT NULL) or ( college_uuid IS NOT NULL) or (college_text IS NOT NULL) or (year_of_passing IS NOT NULL)) and (deleted = 0 or deleted is NULL) "

        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range,
                                                           with_semicolon=False)
        sql_query += " ORDER BY CASE WHEN year_of_passing IS NULL OR year_of_passing=0 THEN 1 else 0 END DESC, year_of_passing DESC, updated_at DESC;"
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        qualification_dict = map_dict["qualification"]
        specialization_dict = map_dict["specialization"]
        college_dict = map_dict["college"]

        profile_education_details = defaultdict(lambda: [])
        for row in db_data:
            profile_education_details[row["profile_id"]].append({
                "id": int(row["id"]),
                "passing_year": row["year_of_passing"],
                "college": {
                    "uuid": row["college_uuid"],
                    "text": college_dict[row["college_uuid"]] if college_dict.get(
                        row["college_uuid"]) and row["college_uuid"] != OTHER_COLLEGE else
                    row["college_text"] if row["college_text"] else college_dict.get(
                        row["college_uuid"])
                },
                "degree": {
                    "uuid": row["highest_qualification_uuid"],
                    "text": qualification_dict[
                        row["highest_qualification_uuid"]] if qualification_dict.get(
                        row["highest_qualification_uuid"]) and row[
                                                                  "highest_qualification_uuid"] != OTHER_HIGHEST_QUALIFICATION else
                    row["highest_qualification_text"] if row[
                        "highest_qualification_text"] else qualification_dict.get(
                        row["highest_qualification_uuid"])
                },
                "specialization": {
                    "uuid": row["specialization_uuid"],
                    "text": specialization_dict[
                        row["specialization_uuid"]] if specialization_dict.get(
                        row["specialization_uuid"]) and row[
                                                           "specialization_uuid"] not in OTHER_SPECIALIZATIONS else
                    row["specialization_text"] if row[
                        "specialization_text"] else specialization_dict.get(
                        row["specialization_uuid"])
                }

            })

        return profile_education_details

    def getUserDisabilities(user_ids, map_dict, user_range=None):
        if user_ids:
            sql_query = "select user_id, disability_type_uuid, TRIM(disability_type_text) as disability_type_text, disability_sub_type_uuid, TRIM(disability_sub_type_text) as disability_sub_type_text, disability_detail_uuid, TRIM(disability_detail_text) as disability_detail_text from falcon.user_disabilities where (deleted = 0 or deleted is NULL) and user_id in " + user_ids + ";"
        elif user_range:
            sql_query = "select user_id, disability_type_uuid, disability_type_text, disability_sub_type_uuid, disability_sub_type_text, disability_detail_uuid, disability_detail_text  from falcon.user_disabilities where (deleted = 0 or deleted is NULL) and user_id >= %d and user_id < %d;" % (
                user_range[0], user_range[1])
        else:
            raise Exception("Params not set")
        disability_type_dict = map_dict["disability_type"]
        disability_detail_dict = map_dict["disability_detail"]
        user_disabilities = defaultdict(lambda: [])
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)

        # TODO need to add code for master dict for text
        for row in db_data:
            user_disabilities[row["user_id"]].append({
                "type": {
                    "uuid": row["disability_type_uuid"],
                    "text": disability_type_dict[
                        row["disability_type_uuid"]] if disability_type_dict.get(
                        row["disability_type_uuid"]) else row["disability_type_text"],
                },
                "sub_type": {
                    "uuid": row["disability_sub_type_uuid"],
                    "text": disability_type_dict[
                        row["disability_sub_type_uuid"]] if disability_type_dict.get(
                        row["disability_sub_type_uuid"]) else row[
                        "disability_sub_type_text"],
                },
                "details": {
                    "uuid": row["disability_detail_uuid"],
                    "text": disability_detail_dict[
                        row["disability_detail_uuid"]] if disability_detail_dict.get(
                        row["disability_detail_uuid"]) else row["disability_detail_text"],
                }
            })
        return user_disabilities

    def getParsedData(profile_ids, map_dict, user_range=None, profile_range=None):

        sql_query = "select profile_id, parsed_experience, parsed_current_designation, parsed_skills from resume_parsed_data_prod where "

        sql_query = FalconDataUtils.appendProfileIdsClause(sql_query, profile_ids,
                                                           user_range, profile_range,
                                                           with_semicolon=True,
                                                           with_and=False)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        resume_parsed_data = {}
        for row in db_data:
            if row["parsed_experience"] is not None and row["parsed_experience"] > 0:
                exp_years = int(row["parsed_experience"])
                exp_months = int(round((row["parsed_experience"] % 1) * 12, 0))
                parsed_experience = Utils.joinAtDecimal(exp_years, exp_months)
            else:
                parsed_experience = None

            resume_parsed_data[row["profile_id"]] = {
                "experience": parsed_experience,
                "current_designation": row["parsed_current_designation"],
                "skills": row["parsed_skills"]
            }
        return resume_parsed_data

    def getParsedDataKiwi(kiwi_profile_ids, map_dict):

        sql_query = "select profile_id, parsed_experience, parsed_current_designation, parsed_skills from resume_parsed_data where kiwi_profile_id in (%s);" % ",".join(
            kiwi_profile_ids)

        db_data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(), sql_query,
                                                 -1, dictionary=True)
        resume_parsed_data = {}
        for row in db_data:
            if row["parsed_experience"] is not None and row["parsed_experience"] > 0:
                exp_years = int(row["parsed_experience"])
                exp_months = int(round((row["parsed_experience"] % 1) * 12, 0))
                parsed_experience = Utils.joinAtDecimal(exp_years, exp_months)
            else:
                parsed_experience = None

            resume_parsed_data[row["profile_id"]] = {
                "experience": parsed_experience,
                "current_designation": row["parsed_current_designation"],
                "skills": row["parsed_skills"]
            }
        return resume_parsed_data
