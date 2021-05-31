import sys
import argparse
import time
import traceback
from datetime import datetime
import multiprocessing as mp
from pebble import ProcessPool, ProcessExpired
from concurrent.futures import TimeoutError

# sys.path.append("/home/ashwin.euler/code_restructured_new/utils")
# from utils.dbutils import DBUtils
from utils.masterdictionary import MasterDictionary
from utils.esutils import ESUtils
from utils.utils import Utils
from utils.constants import SITE_CONTEXTS

# sys.path.append("/home/ashwin.euler/code_restructured_new/utils/data_utils")
from utils.data_utils.falcon_data_utils import FalconDataUtils
from utils.data_utils.bazooka_data_utils import BazookaDataUtils
from utils.data_utils.rio_data_utils import RioDataUtils
from utils.data_utils.common_utils import CommonUtils
from swap_livecore_alias import SwapAlias


def getUsersData(user_ids, user_range, map_dict=None):
    # print("Preparing Master Dictionary")
    if not map_dict:
        map_dict = MasterDictionary.getMapData()
    # print("Done Preparing Master Dictionary")

    user_details = FalconDataUtils.getUsersandDetails(user_ids=user_ids, map_dict=map_dict, user_range=user_range)
    if not user_details:
        print("No users to index for range %d %d" % (user_range[0], user_range[1]))
        return None

    work_auths = FalconDataUtils.getUserWorkPermits(user_ids=user_ids, map_dict=map_dict, user_range=user_range)
    prs = FalconDataUtils.getUserPR(user_ids=user_ids, map_dict=map_dict, user_range=user_range)
    hide_from_employers = FalconDataUtils.getHideEmployers(user_ids=user_ids, map_dict=map_dict, user_range=user_range)
    disabilities = FalconDataUtils.getUserDisabilities(user_ids=user_ids, map_dict=map_dict, user_range=user_range)

    # TODO commenting the below 2 lines as they belonng to bazooka2
    last_active_time = FalconDataUtils.getUserLastActiveTime(user_ids=user_ids, map_dict=map_dict, user_range=user_range)

    kiwi_2_user_id = {user_detail["kiwi_user_id"]: user_detail["user_id"] for user_id, user_detail in
                      user_details.items()}
    kiwi_user_ids_as_sql_list = "(" + ",".join([str(kiwi_user_id) for kiwi_user_id in kiwi_2_user_id.keys()]) + ")"

    seeker_services = BazookaDataUtils.getUserSeekerServices(kiwi_user_ids_as_sql_list, kiwi_2_user_id)
    invitation_sent_by = BazookaDataUtils.getInvitationSentBy(kiwi_user_ids_as_sql_list, kiwi_2_user_id)

    uuid_2_user_id = {user_detail["uuid"]: user_detail["user_id"] for user_id, user_detail in user_details.items()}
    user_uuids_as_sql_list = "(" + ",".join(["'%s'" % uuid for uuid in uuid_2_user_id.keys()]) + ")"

    rio_details = RioDataUtils.getUserDetailsRio(user_uuids=user_uuids_as_sql_list, map_dict=map_dict,
                                                 uuid_2_user_id=uuid_2_user_id)
    profile_details = getProfilesData(user_ids=user_ids, user_range=user_range, map_dict=map_dict)
    final_user_details = {}
    #print("Length of user details: %d" % len(user_details))

    #with open("non_rio_users.txt", "a+") as f:
    #    for user_id in list(set(list(user_details.keys())) - set(list(rio_details.keys()))):
    #        f.write("%d\n" % user_id)

    for user_id, user_detail in user_details.items():
        if user_id not in rio_details:
            continue
        final_user_details[user_id] = {**user_detail, **rio_details[user_id]}
        final_user_details[user_id]["hide_from_employers"] = hide_from_employers.get(user_id)
        final_user_details[user_id]["disabilities"] = disabilities.get(user_id)
        final_user_details[user_id]["work_auth"] = work_auths.get(user_id)
        final_user_details[user_id]["prs"] = prs.get(user_id)
        final_user_details[user_id]["seeker_services"] = seeker_services.get(user_id)
        final_user_details[user_id]["invitation_sent_by"] = invitation_sent_by.get(user_id, [])
        final_user_details[user_id]["profiles"] = profile_details.get(user_id, [])
        final_user_details[user_id]["profilesCount"] = len(final_user_details[user_id]["profiles"])
        update_times = []
        for profile in final_user_details[user_id]["profiles"]:
            profile["name"] = final_user_details[user_id]["name"]
            profile["mobile_details"] = final_user_details[user_id]["mobile_details"]
            profile["email"] = final_user_details[user_id]["email"]
            profile["is_confidential"] = final_user_details[user_id]["is_confidential"]
            profile["enabled"] = 1
            profile["gender"] = final_user_details[user_id]["gender"]
            if profile["update_time"]:
                update_times.append(profile["update_time"])
        if last_active_time.get(user_id):
            update_times.append(last_active_time[user_id])
        final_user_details[user_id]["last_active_time"] = max(update_times) if update_times else None
        final_user_details[user_id]["last_active_date"] = final_user_details[user_id]["last_active_time"].strftime(
            '%Y-%m-%d') if final_user_details[user_id].get("last_active_time") else None
        sms_sent_by, viewed_by, emailed_by, downloaded_by, follows = Utils.getRecruiterActionsFromProfiles(
            profile_details.get(user_id))
        final_user_details[user_id]["sms_sent_by"] = sms_sent_by
        final_user_details[user_id]["downloaded_by"] = downloaded_by
        final_user_details[user_id]["viewed_by"] = viewed_by
        final_user_details[user_id]["emailed_by"] = emailed_by
        final_user_details[user_id]["follows"] = follows
        for site_context in SITE_CONTEXTS:
            max_update_time = datetime.strptime("1970-01-01", "%Y-%m-%d")
            for profile in final_user_details[user_id]["profiles"]:
                if profile["site_context"] == site_context:
                    final_user_details[user_id][site_context] = {
                        "experience": profile["final_experience"],
                        "views": len(profile.get("viewed_by", []))
                    }
                    break
                if site_context in profile["site_contexts_visibility"]:
                    if profile["update_time"]:
                        if max_update_time < profile["update_time"]:
                            final_user_details[user_id][site_context] = {
                                "experience": profile["final_experience"],
                                "views": len(profile.get("viewed_by", []))
                            }
    #print("Length of final user details: %d" % len(final_user_details))
    return final_user_details
    # ESUtils.indexDataInES(ESUtils.esConn(), index_name, final_user_details.values())


def _indexUsersData(user_range=None, user_ids=None):
    final_user_details = getUsersData(user_ids, user_range, map_dict)
    if final_user_details:
        ESUtils.indexDataInES(ESUtils.esConn(), index_name, final_user_details.values())


def getProfilesData(user_ids=None, user_range=None, profile_range=None, map_dict=None):
    if not map_dict:
        map_dict = MasterDictionary.getMapData()
    profile_details = FalconDataUtils.getProfileandDetails(user_ids=user_ids, map_dict=map_dict, user_range=user_range,
                                                           profile_range=profile_range)

    profile_2_site_context = {profile["profile_id"]: profile["site_context"] for user_id, profiles in
                              profile_details.items() for profile in profiles}

    if not profile_details:
        print("No profiles to update")
        return profile_details

    profile_ids = "(" + ", ".join(
        [str(profile["profile_id"]) for user_id, profiles in profile_details.items() for profile in profiles]) + ")"
    profile_languages = FalconDataUtils.getProfileLanguages(profile_ids=profile_ids, map_dict=map_dict,
                                                            user_range=user_range, profile_range=profile_range)
    profile_skills, profile_it_skills = FalconDataUtils.getProfileSkills(profile_ids=profile_ids, map_dict=map_dict,
                                                                         user_range=user_range,
                                                                         profile_range=profile_range)
    profile_courses_and_certifications = FalconDataUtils.getProfileCourseandCertifications(profile_ids=profile_ids,
                                                                                           user_range=user_range,
                                                                                           profile_range=profile_range)
    profile_projects = FalconDataUtils.getProfileProjects(profile_ids=profile_ids, user_range=user_range,
                                                          profile_range=profile_range)

    profile_preferred_locations, profile_preferred_countries = FalconDataUtils.getProfilePreferredLocationsAndCountries(profile_ids=profile_ids,
                                                                                           map_dict=map_dict, user_range=user_range,
                                                                                           profile_range=profile_range)
    profile_preferred_industries = FalconDataUtils.getProfilePreferredIndustries(profile_ids=profile_ids,
                                                                                 map_dict=map_dict,
                                                                                 user_range=user_range,
                                                                                 profile_range=profile_range)
    profile_preferred_roles = FalconDataUtils.getProfilePreferredRoles(profile_ids=profile_ids, map_dict=map_dict,
                                                                       user_range=user_range,
                                                                       profile_range=profile_range)
    profile_preferred_job_types, profile_preferred_employment_types = FalconDataUtils.getProfilePreferredJobEmploymentType(
        profile_ids=profile_ids, map_dict=map_dict, user_range=user_range, profile_range=profile_range)

    profile_current_employment, profile_previous_employments = FalconDataUtils.getEmploymentDetails(
        profile_ids=profile_ids, map_dict=map_dict, user_range=user_range, profile_range=profile_range, profile_2_site_context=profile_2_site_context)
    profile_education_details = FalconDataUtils.getEducationDetails(profile_ids=profile_ids, map_dict=map_dict,
                                                                    user_range=user_range, profile_range=profile_range)

    # User TODO need to change it to kiwi profile ids
    kiwi_2_profile_id = {profile["kiwi_profile_id"]: profile["profile_id"] for user_id, profiles in
                         profile_details.items() for profile in profiles}
    kiwi_profile_ids = [str(profile["kiwi_profile_id"]) for user_id, profiles in profile_details.items() for profile in
                        profiles]

    resume_parsed_data = FalconDataUtils.getParsedDataKiwi(kiwi_profile_ids=kiwi_profile_ids, map_dict=map_dict)

    profile_2_site_context = {profile["profile_id"]: profile["site_context"] for user_id, profiles in
                              profile_details.items() for profile in profiles}

    profile_site_contexts_visibility = CommonUtils.getSiteContextsVisibility(
        profile_ids=list(profile_2_site_context.keys()), map_dict=map_dict,
        profile_2_site_context=profile_2_site_context, profile_preferred_locations=profile_preferred_locations)


    profile_follows, profile_viewed_by, profile_downloaded_by, profile_emailed_by, profile_sms_sent_by, profile_comments = indexRecruiterActions(
        kiwi_profile_ids=kiwi_profile_ids, kiwi_2_profile_id=kiwi_2_profile_id)

    for user_id, profiles in profile_details.items():
        for profile in profiles:
            profile["languages"] = profile_languages.get(profile["profile_id"], [])
            profile["is_searchable"] = 1
            profile["has_data"] = 1
            profile["skills"] = profile_skills.get(profile["profile_id"])
            profile["skills_unnested"] = profile_skills.get(profile["profile_id"])
            profile["it_skills"] = profile_it_skills.get(profile["profile_id"])
            profile["courses_and_certifications"] = profile_courses_and_certifications.get(profile["profile_id"])
            profile["projects"] = profile_projects.get(profile["profile_id"])
            profile["preferred_locations"] = profile_preferred_locations.get(profile["profile_id"])
            profile["preferred_countries"] = profile_preferred_countries.get(profile["profile_id"]) if profile["profile_id"] in profile_preferred_countries else []
            profile["preferred_industries"] = profile_preferred_industries.get(profile["profile_id"])
            profile["preferred_roles"] = profile_preferred_roles.get(profile["profile_id"], [])
            profile["preferred_roles_unnested"] = profile_preferred_roles.get(profile["profile_id"], [])
            profile["preferred_job_types"] = profile_preferred_job_types.get(profile["profile_id"])
            profile["preferred_employment_types"] = profile_preferred_employment_types.get(profile["profile_id"])
            profile["final_experience"] = profile["experience"]

            curr_emp = None
            if profile_current_employment.get(profile["profile_id"]):
                curr_emp = profile_current_employment.get(profile["profile_id"])
                final_ctc, final_usd_ctc = Utils.finalCTC(map_dict, profile["current_salary_mode_uuid"],
                                                          profile["current_salary_currency_code"],
                                                          profile["current_salary_absolute_value"],
                                                         profile["site_context"])
                curr_emp.update(
                    {"final_ctc": final_ctc, "usd_ctc": final_usd_ctc, "ctc": profile["current_salary_absolute_value"],
                     "currency_code": profile["current_salary_currency_code"],
                     "salary_mode": map_dict["salary_mode"].get(profile["current_salary_mode_uuid"])})
            profile["current_employment"] = curr_emp

            if resume_parsed_data.get(profile["profile_id"]):
                parsed_data = resume_parsed_data.get(profile["profile_id"])
                profile["resume_parsed_data"] = {}
                if not profile.get("experience") and parsed_data.get("experience") and parsed_data["experience"] > 0 and parsed_data["experience"] < 100:
                    profile["final_experience"] = parsed_data["experience"]
                    profile["resume_parsed_data"]["experience"] = parsed_data["experience"]
                if ((not profile.get("current_employment")) or (not profile["current_employment"].get("designation"))  or (not profile["current_employment"]["designation"].get("text"))) and parsed_data.get("current_designation") and parsed_data["current_designation"].strip() != "-1" and parsed_data["current_designation"].strip() != "-2":
                    profile["resume_parsed_data"]["current_designation"] = parsed_data["current_designation"]
                if parsed_data.get("skills") and parsed_data["skills"].strip() != "-1" and parsed_data["skills"].strip() != "-2":
                    profile["resume_parsed_data"]["skills"] = parsed_data["skills"]
                    skills_splitted_object = []
                    for splitted_skill in  parsed_data["skills"].split(","):
                        skills_splitted_object.append({"text": splitted_skill})
                    profile["resume_parsed_data"]["skills_splitted_unnested"] = skills_splitted_object
                    profile["resume_parsed_data"]["skills_splitted"] = skills_splitted_object

            profile["previous_employments"] = profile_previous_employments.get(profile["profile_id"])
            profile["educations"] = profile_education_details.get(profile["profile_id"])
            profile["educations_unnested"] = profile_education_details.get(profile["profile_id"])
            profile["site_contexts_visibility"] = profile_site_contexts_visibility.get(profile["profile_id"])
            profile["downloaded_by"] = profile_downloaded_by.get(profile["profile_id"], [])
            profile["viewed_by"] = profile_viewed_by.get(profile["profile_id"], [])
            profile["emailed_by"] = profile_emailed_by.get(profile["profile_id"], [])
            profile["sms_sent_by"] = profile_sms_sent_by.get(profile["profile_id"], [])
            profile["follows"] = profile_follows.get(profile["profile_id"], [])
            profile["comments"] = profile_comments.get(profile["profile_id"], [])

    return profile_details


def indexRecruiterActions(user_range=None, profile_range=None, kiwi_profile_ids=None, kiwi_2_profile_id=None):
    profile_follows, profile_viewed_by, profile_downloaded_by, profile_emailed_by, profile_sms_sent_by = {}, {}, {}, {}, {}
    if not kiwi_profile_ids:
        return profile_follows, profile_viewed_by, profile_downloaded_by, profile_emailed_by, profile_sms_sent_by

    kiwi_profile_ids_sql_list = "(" + ", ".join(kiwi_profile_ids) + ")"

    profile_follows = BazookaDataUtils.getProfileFollows(kiwi_profile_ids_sql_list, kiwi_2_profile_id)
    profile_viewed_by = BazookaDataUtils.getProfileViewedBy(kiwi_profile_ids_sql_list, kiwi_2_profile_id)
    profile_downloaded_by = BazookaDataUtils.getProfileDownloadedBy(kiwi_profile_ids_sql_list, kiwi_2_profile_id)
    profile_emailed_by, profile_sms_sent_by = BazookaDataUtils.getProfileContactedBy(kiwi_profile_ids_sql_list,
                                                                                     kiwi_2_profile_id)
    profile_comments = BazookaDataUtils.getProfileComments(kiwi_profile_ids_sql_list, kiwi_2_profile_id)
    return profile_follows, profile_viewed_by, profile_downloaded_by, profile_emailed_by, profile_sms_sent_by, profile_comments


def _indexDataParallel(num_processes, func, user_ranges, timeout):
    user_ranges_to_index = user_ranges
    with ProcessPool(max_workers=num_processes) as pool:
        user_ranges_to_index_failed = []
        while user_ranges_to_index:
            if user_ranges_to_index_failed:
                print("Starting reindexing of failed user ranges again")
            future = pool.map(func, user_ranges_to_index, timeout=timeout)
            iterator = future.result()
            user_ranges_to_index_failed = []
            index = 0
            while True:
                try:
                    result = next(iterator)
                    print("Indexed user range: %d, %d" % user_ranges_to_index[index])
                except StopIteration:
                    break
                except ProcessExpired as error:
                    print("ProcessExpired: Adding user range (%d, %d) into failed ranges" % user_ranges_to_index[index])
                    user_ranges_to_index_failed.append(user_ranges_to_index[index])
                except TimeoutError as error:
                    print("TimeoutError: Adding user range (%d, %d) into failed ranges" % user_ranges_to_index[index])
                    user_ranges_to_index_failed.append(user_ranges_to_index[index])
                except Exception:
                    print(traceback.format_exc())
                    print("Exception: Adding user range (%d, %d) into failed ranges" % user_ranges_to_index[index])
                    user_ranges_to_index_failed.append(user_ranges_to_index[index])
                finally:
                    index += 1
            user_ranges_to_index = user_ranges_to_index_failed


def indexDataParallel(rows_per_process, count=None):
    num_processes = 8 * mp.cpu_count()
    print("No of processes: %d" % num_processes)
    if count:
        max_user_id = count
    else:
        max_user_id = FalconDataUtils.getMaxUserId()
    print("Max user id: %d" % max_user_id)
    user_ranges = [(i, i + rows_per_process) for i in range(1, max_user_id + 1, rows_per_process)]
    print("Total User ranges: %d" % len(user_ranges))
    num_processes = min(num_processes, len(user_ranges))
    print("Revised no of processes: %d" % num_processes)
    print("Number of processes cannot be greater than 128")
    num_processes = min(num_processes, 128)
    _indexDataParallel(num_processes, _indexUsersData, user_ranges, 18 * 60)
    # _indexDataParallel(num_processes, indexProfilesData, user_ranges, 15*60)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index-name", default="users", help="index name")
    parser.add_argument("-u", "--users-count", help="Number of users to index", type=int)
    parser.add_argument("--only-create-index", action='store_true', help="Only create index")
    parser.add_argument("--force-create", action='store_true', help="This will delete the index if already present")
    parser.add_argument("--yin-yang", action='store_true')
    parser.add_argument("--index-all-data", action='store_true',
                        help="This option si set when we want to index whole data")
    parser.add_argument("--rows-per-process", default=8000, help="this is number of users assigned to each thread",
                        type=int)
    parser.add_argument("--swap-alias", default=False, help="Whether swap alias livecore or not", action="store_true")

    argv = vars(parser.parse_args())

    es_conn = ESUtils.esConn()
    ESUtils.add_stored_scripts(es_conn)
    print("es_conn: %s" % str(es_conn))
    if argv.get("yin_yang"):
        index_name = ESUtils.get_active_inactive_indexes(es_conn, 'livecore')["inactive_index"]
    else:
        index_name = argv["index_name"]

    users_count = argv.get("users_count")
    rows_per_process = argv["rows_per_process"]

    ESUtils.createESIndex(es_conn, index_name, argv["force_create"])
    if argv["only_create_index"]:
        sys.exit()

    # File= open("function_logs.txt","w")
    print("Preparing master dictionary")
    map_dict = MasterDictionary.getMapData()
    print("Done Preparing master dictionary")
    start_time = time.time()
    print("Starting the indexer at %s" % str(start_time))
    if users_count:
        indexDataParallel(rows_per_process, users_count)
    elif argv["index_all_data"]:
        indexDataParallel(rows_per_process)
    # File.write("Total time taken to index %d documents is %s seconds" % (users_count, str(time.time() - start_time)))
    ESUtils.updateRefreshReplicasSettings(es_conn, index_name)
    if users_count:
        print("Total time taken to index %d documents is %s seconds" % (users_count, str(time.time() - start_time)))
    else:
        print("Total time taken to index all documents is %s seconds" % (str(time.time() - start_time)))
    if argv["swap_alias"]:
        SwapAlias.execute()
