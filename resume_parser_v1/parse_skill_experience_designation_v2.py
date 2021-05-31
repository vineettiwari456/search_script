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

print("Start Processing Time : ", datetime.now())
main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "txtfiles")


class ExtractSkillExperienceDes:
    def __init__(self):
        current_time = datetime.now()
        d = datetime(current_time.year, current_time.month, current_time.day, 5, 30, 00)
        self.maxdate = int(time.mktime(d.timetuple())) * 1000
        self.chunk_size=2000
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
        self.partition_size = 100
        self.processes = 20

    def get_profiles(self, profile_id):
        profile_ids = ','.join(profile_id)
        query = """
        SELECT id, kiwi_profile_id, user_id, resume_file_path 
        FROM user_profiles
        WHERE
                id in ({profile_ids}) AND
                resume_file_path IS NOT NULL;
        """.format(
            profile_ids=profile_ids
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
        # start, end = args
        results = []
        truncated_ids = []
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
            print("++=====+++: ", len(results))
            # if results:
            #     self.obj_db.my_query(self.obj_db.get_falcon_write_db(), self.skills_update_query, results, get=False)

        # truncated_ids = ", ".join(truncated_ids)
        # print("*" * 100)
        # self.obj_db.my_query(
        #     self.obj_db.get_parser_db(),
        #     self.skills_sync_query.format(self.skills_function_name, start, end, truncated_ids),
        #     get=False,
        # )

    def divide_chunks(self, l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def get_included_by_fn(self, fn_name):
        excluded = self.obj_db.my_query(
            self.obj_db.get_parser_db(),
            f"SELECT start, end FROM processed WHERE function = "
            f"'{fn_name}'ORDER BY start , end DESC;",
        )
        return self.obj_db.partition(self.obj_db.get_included(excluded, *self.get_ids()), self.partition_size)

    def start_process(self):
        last_activeatinsert_id = main_directory + "/last_active_at"
        read_file = open(last_activeatinsert_id)
        last_active_at = read_file.read()
        read_file.close()
        print(last_active_at)
        lastactive_at_time = int(last_active_at)
        print("start time :", lastactive_at_time)
        is_next = True
        # start_date=1614796200000
        while lastactive_at_time <= self.maxdate and is_next:
            # query = "select uad.profile_id,uad.active_at from user_active_data as uad where uad.profile_id is not null and uad.active_at between {0} and {1} order by uad.active_at asc LIMIT {2};".format(
            #     lastactive_at_time, self.maxdate, 20000)
            query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where up.resume_exists=1 and uad.active_at between {0} and {1} order by uad.active_at asc LIMIT {2};".format(
                lastactive_at_time, self.maxdate, self.chunk_size)
            print(query)
            rows = self.obj_db.my_query(self.obj_db.get_falcon_db(), query, dictionary=True)
            profile_ids = []
            print(len(rows))
            if len(rows) > 1:
                for row in rows:
                    lastactive_at_time = row["activedate"]
                    profile_ids.append(str(row["profile_id"]))
                print("lastactive_at_time:", lastactive_at_time)
                if len(profile_ids) > 0:
                    included = list(self.divide_chunks(profile_ids, 100))
                    print(included)
                    with multiprocessing.Pool(processes=self.processes) as pool:
                        with tqdm(total=len(included) * self.partition_size) as pbar:
                            for _ in pool.imap_unordered(self.parse_skills, included):
                                pbar.update(self.partition_size)
                                pbar.refresh()
                last_writeid = open(last_activeatinsert_id, 'w')
                last_writeid.write(str(lastactive_at_time))
                last_writeid.close()
            else:
                is_next = False

        print("Task Completed : ", datetime.now())


if __name__ == '__main__':
    obj = ExtractSkillExperienceDes()
    obj.start_process()
