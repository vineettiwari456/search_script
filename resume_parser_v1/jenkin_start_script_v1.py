#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

from util import DbUtils
import os
import psutil
PROCNAME = "parse_skill_experience_designation_full.py"
FROM_START = str(os.environ.get("FROM_START"))
NUMBER_OF_THREADS = os.environ.get("NUMBER_OF_THREADS")
WITH_DESIGNATION = os.environ.get("WITH_DESIGNATION")
print(FROM_START,NUMBER_OF_THREADS)


class StartJenkinsJobs:
    def __init__(self):
        self.obj_db = DbUtils()
        if str(NUMBER_OF_THREADS) == 'None':
            self.NUMBER_OF_THREADS = 20
        else:
            self.NUMBER_OF_THREADS = int(NUMBER_OF_THREADS)
        if str(WITH_DESIGNATION).lower()=='yes':
            self.WITH_DESIGNATION = str(self.NUMBER_OF_THREADS)+"-thread_with_designation"
        else:
            self.WITH_DESIGNATION = str(self.NUMBER_OF_THREADS) + "-thread_without_designation"

    def start(self):
        try:
            query = "update replication_tables set descr='YES',start_time=now(),last_active_at={0} where code='FULL_RESUME_PARSER'".format(self.WITH_DESIGNATION)

            self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), query, get=False)
            for proc in psutil.process_iter():
                if len(proc.cmdline()) > 0:
                    if PROCNAME in proc.cmdline()[-1]:
                        print(proc.pid, proc.cmdline()[-1])
                        os.kill(proc.pid, 9)
            if FROM_START.strip().lower() == "yes":
                for arg in range(0, 10):
                    code = "RESUME_TEXT_" + str(arg)
                    dolphin_query = "update replication_tables set last_active_at='0' where code='{0}'".format(code)
                    self.obj_db.my_query(self.obj_db.get_dolphin_write_db(), dolphin_query, get=False)
        except Exception as e:
            raise Exception(e)


if __name__ == '__main__':
    obj = StartJenkinsJobs()
    obj.start()
