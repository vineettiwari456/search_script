#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

from util import DbUtils
from skills_lib import SkillExtraction
import time
import multiprocessing
from datetime import datetime
from tqdm.notebook import tqdm
import zlib, sys, os
from sys import getsizeof
from experience_all_resume import get_years_of_experience_text
from collections import defaultdict
import psutil

startdate_jobs = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start Processing Time : ", startdate_jobs)

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "txtfiles")


# resume_insertpid = main_directory + "/resume_insert_pid"
# fd = open(resume_insertpid)
# pidval = fd.read()
# fd.close()
# if pidval:
#     if int(pidval) in [p.info["pid"] for p in psutil.process_iter(attrs=['pid'])]:
#         print('Process is already running------', pidval)
#         exit(0)
# pidfile = open(resume_insertpid, 'w')
# pidfile.write(str(os.getpid()))
# pidfile.close()


class ExtractSkillExperienceDes:
    def __init__(self):
        self.obj_db = DbUtils()
        self.obj_skill = SkillExtraction()
        self.skills_update_query = """
        INSERT INTO 
            falcon.resume_parsed_data 
            (kiwi_profile_id, profile_id, user_id, resume_url, parsed_skills,parsed_experience) 
        VALUES 
            (%(kiwi_profile_id)s, %(profile_id)s, %(user_id)s, %(resume_url)s, %(parsed_skills)s,%(parsed_experience)s) 
        ON DUPLICATE KEY UPDATE 
        parsed_skills=VALUES(parsed_skills),parsed_experience=VALUES(parsed_experience);
        """

        self.skills_sync_query = """
        INSERT INTO processed (function, start, end, truncated_ids)
        VALUES ('{}', {}, {}, '{}');"""
        self.partition_size = 2000
        self.processes = 20

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
        # profiles = []
        # if len(rows) > 0:
        #     for row in rows:
        #         profiles.append({
        #             "id": row["id"],
        #             "kiwi_profile_id": row["kiwi_profile_id"],
        #             "user_id": row["user_id"],
        #             "resume_url": row["resume_file_path"],
        #             # 'resume_text': zlib.decompress(dict[row["id"]].encode("latin1")).decode(
        #             #     "utf-8"
        #             # )
        #         })

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
        # print(args)
        # start, end = args
        results = []
        truncated_ids=[]
        profiles = self.get_profiles(args)
        print("Length of Profiles : : ", len(profiles))
        # profiles={91027951:""}
        resumes = self.get_resumes(profiles.keys())
        print("Length of resumes found : : ", len(resumes))
        if resumes:
            for pid, resume in resumes:
                resume = [line for line in resume.split("\n") if line != ""]
                ex_exp = get_years_of_experience_text(resume)
                cleaned_resume = [self.obj_skill.clean_text(line) for line in resume]
                ex_skills = str(self.obj_skill.match(cleaned_resume) if resume else -1)
                profile = profiles[pid]
                if len(ex_skills) > 65500:  # 65,535 is limit by Mysql
                    ex_skills = ex_skills[: ex_skills.rindex(",", 0, 65500)]
                    truncated_ids.append(str(profile["kiwi_profile_id"]))
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
        # truncated_ids = []
        # resumes = self.get_profiles(args)
        # print("Length of Profiles : : ", len(resumes))
        # if len(resumes) > 0:
        #     for profile in resumes:
        #         resume = [line for line in profile.get("resume_text", "").split("\n") if line != ""]
        #         ex_exp = get_years_of_experience_text(resume)
        #         cleaned_resume = [self.obj_skill.clean_text(line) for line in resume]
        #         ex_skills = str(self.obj_skill.match(cleaned_resume) if resume else -1)
        #         if len(ex_skills) > 65500:  # 65,535 is limit by Mysql
        #             ex_skills = ex_skills[: ex_skills.rindex(",", 0, 65500)]
        #         results.append(
        #             {
        #                 "kiwi_profile_id": str(profile["kiwi_profile_id"]),
        #                 "profile_id": str(profile["id"]),
        #                 "user_id": str(profile["user_id"]),
        #                 "resume_url": str(profile["resume_url"]),
        #                 "parsed_skills": ex_skills,
        #                 "parsed_experience": ex_exp,
        #             }
        #         )
            print("++=====+++: ", len(results), results[:2])
            if results:
                self.obj_db.my_query(self.obj_db.get_falcon_write_db(), self.skills_update_query, results, get=False)
        #

    def get_included_by_fn(self, fn_name):
        excluded = self.obj_db.my_query(
            self.obj_db.get_parser_db(),
            f"SELECT start, end FROM processed WHERE function = "
            f"'{fn_name}'ORDER BY start , end DESC;",
        )
        return self.obj_db.partition(self.obj_db.get_included(excluded, *self.get_ids()), self.partition_size)

    def get_dolphin_records(self, k, lastupdatetime):
        total_data_main = []
        if lastupdatetime:
            query = "select id,kiwi_profile_id,profile_id,updated from user_active_resume_text_{0} where updated>='{1}' order by updated asc limit 2000".format(
                k, lastupdatetime)
            print(query)
            total_data_main = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
        return total_data_main

    def divide_chunks(self, l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def start_process(self):
        # included = self.get_included_by_fn(self.skills_function_name)
        last_activeatinsert_id = main_directory + "/last_active_at_insert_time_0"
        for arg_val in range(0, 1):
            read_file = open(last_activeatinsert_id)
            last_active_at = read_file.read()
            read_file.close()
            print(last_active_at)
            lastactive_at_time = str(last_active_at)
            print("start time :", lastactive_at_time)
            # print(included)
            is_next = True
            id_value = 0
            count=0
            profileids = []
            while lastactive_at_time and is_next:
                records = self.get_dolphin_records(arg_val, lastactive_at_time)
                print(len(records))
                if len(records)<100:
                    count+=1
                if count>5:
                    is_next=False
                if len(records) > 2:
                    for row in records:
                        lastactive_at_time = row["updated"]
                    print(lastactive_at_time)
                    included = list(self.divide_chunks(records, 100))[:]
                    with multiprocessing.Pool(processes=self.processes) as pool:
                        with tqdm(total=len(included) * self.partition_size) as pbar:
                            for _ in pool.imap_unordered(self.parse_skills, included):
                                pbar.update(self.partition_size)
                                pbar.refresh()
                else:
                    is_next = False

        # print(startdate_jobs)
        # last_writeid = open(last_activeatinsert_id, 'w')
        # last_writeid.write(str(startdate_jobs))
        # last_writeid.close()
        print("Task Completed : ", datetime.now())


if __name__ == '__main__':
    obj = ExtractSkillExperienceDes()
    obj.start_process()
