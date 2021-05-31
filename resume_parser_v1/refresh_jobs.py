#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

from util import DbUtils
import os
import psutil

PROCNAME = "parse_skill_experience_designation_full.py"

class StartJenkinsJobs:
    def __init__(self):
        self.obj_db = DbUtils()

    def start(self):
        try:
            for proc in psutil.process_iter():
                if len(proc.cmdline()) > 0:
                    if PROCNAME in proc.cmdline()[-1]:
                        print(proc.pid, proc.cmdline()[-1])
                        os.kill(proc.pid, 9)
        except Exception as e:
            raise Exception(e)


if __name__ == '__main__':
    obj = StartJenkinsJobs()
    obj.start()
