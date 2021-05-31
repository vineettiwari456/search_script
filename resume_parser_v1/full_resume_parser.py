#!/usr/bin/env python
# coding: utf-8
# -*-coding:utf-8 -*-

import concurrent
from parse_skill_experience_designation_full import ExtractSkillExperienceDes
class FullResumeParser:
    def __init__(self):
        self.obj=ExtractSkillExperienceDes()

    def process(self,id):
        self.obj.start_process(id)

    def start(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(self.process, index): index for index in
                             range(10)}
            for future in concurrent.futures.as_completed(future_to_url):
                url2 = future_to_url[future]
                try:
                    data = future.result()
                    if data:
                        Total_data.append(data)
                except Exception as exc:
                    print(exc)
                    pass