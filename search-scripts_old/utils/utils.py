import json
import sys
import argparse
import math
from operator import itemgetter
from .constants import SITE_CONTEXT_2_DEFAULT_CURRENCY


class Utils:

    def finalCTC(map_dict, salary_mode, currency_code, salary, site_context):
        if salary is None or salary == 0:
            return salary, salary
        conversion_to_usd_dict = map_dict["currency_conversion"]
        salary_mode_dict = map_dict["salary_mode"]
        if salary_mode in salary_mode_dict:
            salary_mode = salary_mode_dict[str(salary_mode)]

        if currency_code in conversion_to_usd_dict:
            conversion_rate = conversion_to_usd_dict[currency_code]
            conversion_rate_inr = conversion_rate/conversion_to_usd_dict["INR"]
        elif site_context:
            conversion_rate = conversion_to_usd_dict[SITE_CONTEXT_2_DEFAULT_CURRENCY[site_context]]
            conversion_rate_inr = conversion_rate/conversion_to_usd_dict["INR"]
        else:
            return None, None

        if salary_mode == "Monthly":
            if currency_code == "INR" or ((not currency_code) and site_context == "rexmonster"):
                final_ctc = float(salary) * 12.0
            else:
                final_ctc = float(salary) * 12.0 * conversion_rate_inr
            final_usd_ctc = float(salary) * 12.0 * conversion_rate
        else:
            if currency_code == "INR" or ((not currency_code) and site_context == "rexmonster"):
                final_ctc = float(salary)
            else:
                final_ctc = float(salary) * conversion_rate_inr
            final_usd_ctc = float(salary) * conversion_rate

        return round(final_ctc, 6), round(final_usd_ctc, 6)

    def sortedZipLongest(l1, l2, key, fillvalue={}):
        l1 = iter(sorted(l1, key=lambda x: x[key]))
        l2 = iter(sorted(l2, key=lambda x: x[key]))
        u = next(l1, None)
        v = next(l2, None)

        while (u is not None) or (v is not None):
            if u is None:
                yield fillvalue, v
                v = next(l2, None)
            elif v is None:
                yield u, fillvalue
                u = next(l1, None)
            elif u.get(key) == v.get(key):
                yield u, v
                u = next(l1, None)
                v = next(l2, None)
            elif u.get(key) < v.get(key):
                yield u, fillvalue
                u = next(l1, None)
            else:
                yield fillvalue, v
                v = next(l2, None)

    def mergeDict(l1, l2, joined_key):
        l3 = [{**u, **v} for u, v in Utils.sortedZipLongest(l1, l2, key=joined_key, fillvalue={})]
        return l3

    def combineColumns(my_dict, ID, Value):
        data = {}
        for element in my_dict:
            id = element[ID]
            column_name = element[Value]
            if id not in data:
                data[id] = []
            data[id].append(column_name)
        new_lst = [{ID: key, Value: val} for key, val in data.items()]
        return new_lst

    def joinAtDecimal(a, b):
        return float(a) + float(b * (math.pow(10, -2)))

    def getRecruiterActionsFromProfiles(profiles):
        sms_sent_by, viewed_by, emailed_by, downloaded_by, follows = [], [], [], [], []
        if profiles:
            for profile in profiles:
                sms_sent_by += profile.get("sms_sent_by", [])
                viewed_by += profile.get("viewed_by", [])
                downloaded_by += profile.get("downloaded_by", [])
                emailed_by += profile.get("emailed_by", [])
                follows += profile.get("follows", [])
        return sms_sent_by, viewed_by, emailed_by, downloaded_by, follows

    def makeCompoundField(profile):
        compound_field = ""
        if profile.get("current_employment") and profile["current_employment"].get("designation") and \
                profile["current_employment"]["designation"].get("text"):
            compound_field += profile["current_employment"]["designation"]["text"]
            compound_field += ", "
        if profile.get("title"):
            compound_field += profile["title"]
            compound_field += ", "
        if profile.get("skills"):
            compound_field += ",".join([skill["text"] for skill in profile["skills"] if skill.get("text")])
            compound_field += ", "
        if profile.get("preferred_roles"):
            compound_field += ",".join(
                [function_and_roles["function"]["text"] for function_and_roles in profile["preferred_roles"] if
                 function_and_roles["function"].get("text")])
            compound_field += ", "
            compound_field += ",".join([role["text"] for function_and_roles in profile["preferred_roles"] for role in
                                        function_and_roles["roles"] if role.get("text")])
            compound_field += ", "
        if profile.get("preferred_industries"):
            compound_field += ",".join(
                [industry["text"] for industry in profile["preferred_industries"] if industry.get("text")])
            compound_field += ", "
        return compound_field
