#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

from util import DbUtils
import os
FROM_START = str(os.environ.get("FROM_START"))
print(FROM_START)

class StartJenkinsJobs:
    def __init__(self):
        self.obj_db = DbUtils()
    def start(self):
        try:
            query = "update replication_tables set descr='YES',start_time=now() where code='FULL_RESUME_PARSER'"
            self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), query, get=False)
            if FROM_START.strip().lower() == "yes":
                euler_query="delete from processed where function='parse_exp_skills_full'"
                self.obj_db.my_query(self.obj_db.get_parser_db(), euler_query, get=False)
        except Exception as e:
            raise Exception(e)
if __name__ == '__main__':
    obj=StartJenkinsJobs()
    obj.start()

