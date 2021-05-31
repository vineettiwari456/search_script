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

print("Start Processing Time : ", datetime.now())
main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..", "txtfiles")


class ExtractSkillExperienceDes:
    def __init__(self):
        self.skills_function_name = "parse_exp_skills_full"
        print(self.skills_function_name)
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
        self.processes = 30

    def get_profiles(self, start, end):
        query = """
        SELECT id, kiwi_profile_id, user_id, resume_file_path 
        FROM user_profiles
        WHERE
                kiwi_profile_id > {start} AND
                kiwi_profile_id <= {end} AND
                resume_file_path IS NOT NULL;
        """.format(
            start=start, end=end
        )
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
        start, end = args
        results = []
        truncated_ids = []
        profiles = self.get_profiles(start, end)
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
            print("++=====+++: ", len(results))
            if results:
                self.obj_db.my_query(self.obj_db.get_falcon_write_db(), self.skills_update_query, results, get=False)

        truncated_ids = ", ".join(truncated_ids)
        print("*" * 100)
        self.obj_db.my_query(
            self.obj_db.get_parser_db(),
            self.skills_sync_query.format(self.skills_function_name, start, end, truncated_ids),
            get=False,
        )

    def get_included_by_fn(self, fn_name):
        excluded = self.obj_db.my_query(
            self.obj_db.get_parser_db(),
            f"SELECT start, end FROM processed WHERE function = "
            f"'{fn_name}'ORDER BY start , end DESC;",
        )
        return self.obj_db.partition(self.obj_db.get_included(excluded, *self.get_ids()), self.partition_size)

    def final_insertion_check(self):
        query = "update replication_tables set descr='NO',end_time=now() where code='FULL_RESUME_PARSER'"
        self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), query, get=False)

    def check_full_resume_parser(self):
        is_full=False
        query = "select descr from replication_tables where code='FULL_RESUME_PARSER'"
        rows = self.obj_db.my_query(self.obj_db.get_dolphin_db(), query, dictionary=True)
        if len(rows)>0:
            rawdict = rows[0]
            if str(rawdict.get("descr")).lower()=="yes":
                is_full=True
        return is_full
    def start_process(self):
        included = self.get_included_by_fn(self.skills_function_name)
        print(len(included))
        with multiprocessing.Pool(processes=self.processes) as pool:
            with tqdm(total=len(included) * self.partition_size) as pbar:
                for _ in pool.imap_unordered(self.parse_skills, included):
                    pbar.update(self.partition_size)
                    pbar.refresh()
        print("Task Completed : ", datetime.now())


if __name__ == '__main__':
    obj = ExtractSkillExperienceDes()
    is_start_process = obj.check_full_resume_parser()
    print("Start full resume parser Flag: ", is_start_process)
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
            obj.start_process()
            obj.final_insertion_check()
        except Exception as e:
            raise Exception(e)
