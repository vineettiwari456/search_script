#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

from util import DbUtils
from skills_lib import SkillExtraction
from designation import ExtractDesignation
import time
import multiprocessing
from datetime import datetime
from tqdm.notebook import tqdm
import zlib, sys, os
from sys import getsizeof
from experience_all_resume import get_years_of_experience_text
from collections import defaultdict
import psutil
import concurrent
import concurrent.futures

startdate_jobs = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start Processing Time : ", startdate_jobs)

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "txtfiles")


class ExtractSkillExperienceDes:
    def __init__(self):
        try:
            self.obj_db = DbUtils()
            self.obj_skill = SkillExtraction()
            self.obj_designation = ExtractDesignation()
            self.skills_with_designation_update_query = """
            INSERT INTO
                falcon.resume_parsed_data
                (kiwi_profile_id, profile_id, user_id, resume_url, parsed_skills,parsed_experience,parsed_current_designation)
            VALUES
                (%(kiwi_profile_id)s, %(profile_id)s, %(user_id)s, %(resume_url)s, %(parsed_skills)s,%(parsed_experience)s,%(parsed_current_designation)s)
            ON DUPLICATE KEY UPDATE
            parsed_skills=VALUES(parsed_skills),parsed_experience=VALUES(parsed_experience),parsed_current_designation=VALUES(parsed_current_designation);
            """
            self.skills_without_designation_update_query = """
            INSERT INTO
                falcon.resume_parsed_data
                (kiwi_profile_id, profile_id, user_id, resume_url, parsed_skills,parsed_experience)
            VALUES
                (%(kiwi_profile_id)s, %(profile_id)s, %(user_id)s, %(resume_url)s, %(parsed_skills)s,%(parsed_experience)s)
            ON DUPLICATE KEY UPDATE
            parsed_skills=VALUES(parsed_skills),parsed_experience=VALUES(parsed_experience);
            """
            self.partition_size = 2000
            self.processes = 20
        except Exception as exc:
            raise Exception(exc)

    def get_profiles(self, kiwi_ids_list):
        dict = {}
        for kw in kiwi_ids_list:
            dict[kw.get("profile_id")] = kw.get("resume_text")
        profiles_ids = ','.join(
            [str(kiwi.get("profile_id", "")) for kiwi in kiwi_ids_list if kiwi.get("profile_id", "")])
        query = """
        SELECT id, kiwi_profile_id, user_id, resume_file_path 
        FROM user_profiles
        WHERE
                id in ({profiles_ids}) AND

                resume_file_path IS NOT NULL;
        """.format(
            profiles_ids=profiles_ids
        )
        # print(query)
        rows = self.obj_db.my_query(self.obj_db.get_falcon_db(), query, dictionary=True)
        profiles = {}
        for row in rows:
            profiles[row["id"]] = {
                "id": row["id"],
                "kiwi_profile_id": row["kiwi_profile_id"],
                "user_id": row["user_id"],
                "resume_url": row["resume_file_path"],
            }
        return profiles

    def get_ids(self):
        return self.obj_db.my_query(
            self.obj_db.get_falcon_db(),
            "SELECT MIN(kiwi_profile_id), MAX(kiwi_profile_id) FROM user_profiles;",
        )[0]

    def get_resumes(self, profile_ids):
        partitions = defaultdict(list)
        for profile_id in profile_ids:
            partitions[profile_id % 10].append(profile_id)
        results = []
        for idx, partition in partitions.items():
            query = "SELECT profile_id, resume_text FROM user_active_resume_text_{idx} WHERE profile_id IN ({ids})".format(
                idx=idx, ids=",".join((str(profile_id) for profile_id in partition))
            )
            rows = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
            results += [
                (
                    row["profile_id"],
                    zlib.decompress(row.get("resume_text").encode("latin1")).decode(
                        "utf-8"
                    ),
                )
                for row in rows
            ]
        return results

    def parse_skills(self, args):
        is_with_designation = self.is_with_designation
        results = []
        truncated_ids = []
        profiles = self.get_profiles(args)
        print("Length of Profiles : : ", len(profiles))
        # profiles={91027951:""}
        resumes = self.get_resumes(profiles.keys())
        print("Length of resumes found : : ", len(resumes))
        if resumes:
            for pid, resume in resumes:
                ex_exp = get_years_of_experience_text(resume)
                resume = [line for line in resume.split("\n") if line != ""]
                if is_with_designation:
                    ex_designation = self.obj_designation.get_designaiton(resume)
                cleaned_resume = [self.obj_skill.clean_text(line) for line in resume]
                ex_skills = str(self.obj_skill.match(cleaned_resume) if resume else -1)
                profile = profiles[pid]
                if len(ex_skills) > 65500:  # 65,535 is limit by Mysql
                    ex_skills = ex_skills[: ex_skills.rindex(",", 0, 65500)]
                    truncated_ids.append(str(profile["kiwi_profile_id"]))
                if is_with_designation:
                    results.append(
                        {
                            "kiwi_profile_id": str(profile["kiwi_profile_id"]),
                            "profile_id": str(profile["id"]),
                            "user_id": str(profile["user_id"]),
                            "resume_url": str(profile["resume_url"]),
                            "parsed_skills": ex_skills,
                            "parsed_experience": ex_exp,
                            "parsed_current_designation": ex_designation,
                        }
                    )
                else:
                    results.append(
                        {
                            "kiwi_profile_id": str(profile["kiwi_profile_id"]),
                            "profile_id": str(profile["id"]),
                            "user_id": str(profile["user_id"]),
                            "resume_url": str(profile["resume_url"]),
                            "parsed_skills": ex_skills,
                            "parsed_experience": ex_exp,

                        }
                    )
            if is_with_designation:
                skills_update_query = self.skills_with_designation_update_query
            else:
                skills_update_query = self.skills_without_designation_update_query
            print("Inserted/updated resume length: :", len(results))
            if results:
                self.obj_db.my_query(self.obj_db.get_falcon_write_db(), skills_update_query, results, get=False)

    def get_dolphin_records(self, k, lastupdatetime):
        total_data_main = []
        if lastupdatetime:
            query = "select id,kiwi_profile_id,profile_id,updated from user_active_resume_text_{0} where id>{1} order by id asc limit {2}".format(
                k, lastupdatetime, self.partition_size)
            print(query)
            total_data_main = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
        return total_data_main

    def divide_chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def final_insertion_check(self):
        query = "update replication_tables set descr='NO',end_time=now() where code='FULL_RESUME_PARSER'"
        self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), query, get=False)

    def check_full_resume_parser(self):
        is_full = False
        is_with_designation = False
        number_of_thread = 20
        query = "select descr,last_active_at from replication_tables where code='FULL_RESUME_PARSER'"
        rows = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
        if len(rows) > 0:
            rawdict = rows[0]
            if str(rawdict.get("descr")).lower() == "yes":
                is_full = True
            if rawdict.get("last_active_at"):
                numthreda = str(rawdict.get("last_active_at")).split("-")
                number_of_thread = int(numthreda[0].strip())
                if 'with_designation' in numthreda[-1]:
                    is_with_designation = True
                # number_of_thread=int(rawdict.get("last_active_at"))
        return is_full, number_of_thread, is_with_designation

    def get_last_id_replication_table(self, arg):
        last_id = 0
        code = "RESUME_TEXT_" + str(arg)
        query = "select last_active_at from replication_tables where code='{0}'".format(
            code)
        print(query)
        total_data_main = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
        if len(total_data_main) > 0:
            last_id = total_data_main[0].get("last_active_at")
        return last_id

    def update_replication_table(self, last_id, arg, is_start=False, is_end=False):
        code = "RESUME_TEXT_" + str(arg)
        if is_start:
            query = "update replication_tables set descr='1',start_time=now() where code='{0}'".format(code)
        elif is_end:
            query = "update replication_tables set descr='0',end_time=now() where code='{0}'".format(code)
        else:
            query = "update replication_tables set last_active_at={0},end_time=now() where code='{1}'".format(last_id,
                                                                                                              code)
        self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), query, get=False)

    def start_process(self, arg_val, number_of_threads, is_with_designation):
        self.is_with_designation = is_with_designation
        last_active_at = self.get_last_id_replication_table(arg_val)
        lastactive_at_time = str(last_active_at)
        self.update_replication_table(lastactive_at_time, arg_val, is_start=True)
        print("start id :", arg_val, lastactive_at_time)
        is_next = True
        count = 0
        while lastactive_at_time and is_next:
            records = self.get_dolphin_records(arg_val, lastactive_at_time)
            print(len(records))
            if len(records) > 0:
                for row in records:
                    lastactive_at_time = row["id"]
                print(lastactive_at_time)
                included = list(self.divide_chunks(records, 100))
                with multiprocessing.Pool(processes=number_of_threads) as pool:
                    with tqdm(total=len(included) * self.partition_size) as pbar:
                        for _ in pool.imap_unordered(self.parse_skills, included):
                            pbar.update(self.partition_size)
                            pbar.refresh()
                self.update_replication_table(lastactive_at_time, arg_val)
            else:
                is_next = False
                self.update_replication_table(lastactive_at_time, arg_val, is_end=True)

        print("started Time:: ", startdate_jobs)

        print("Task Completed : ", datetime.now())


if __name__ == '__main__':
    obj = ExtractSkillExperienceDes()
    is_start_process, number_of_threads, is_with_designation = obj.check_full_resume_parser()
    print("Start full resume parser Flag: ", is_start_process, number_of_threads, is_with_designation)
    # is_start_process = True
    # is_with_designation = True
    if is_start_process:
        try:
            resume_insertpid = main_directory + "/resume_migration_pid"
            fd = open(resume_insertpid)
            pidval = fd.read()
            fd.close()
            if pidval:
                if int(pidval) in [p.info["pid"] for p in psutil.process_iter(attrs=['pid'])]:
                    print('Process is already running------', pidval)
                    exit(0)
            pidfile = open(resume_insertpid, 'w')
            pidfile.write(str(os.getpid()))
            pidfile.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_url = {
                    executor.submit(obj.start_process, index, number_of_threads, is_with_designation): index for index
                    in
                    range(0, 10)}
                for future in concurrent.futures.as_completed(future_to_url):
                    url2 = future_to_url[future]
                    data = future.result()
                    if data:
                        print(data)
            obj.final_insertion_check()
        except Exception as e:
            raise Exception(e)
