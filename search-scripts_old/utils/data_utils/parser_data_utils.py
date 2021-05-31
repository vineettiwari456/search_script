import json
import sys
import argparse
import math
from operator import itemgetter
from collections import defaultdict

from ..dbutils import DBUtils
from ..utils import Utils
from .common_utils import CommonUtils

class ParserDataUtils:

    def getExperience(profile_ids, user_range, profile_range):
        sql_query = "select * from parser_prod where (((exp_years=0 and exp_months=0) or (exp_years is null and exp_months is null)) and Experience>=0) "
        sql_query = CommonUtils.appendProfileIdsClause(sql_query, profile_ids, user_range, profile_range)
        db_data = DBUtils.fetch_results_in_batch(DBUtils.parser_db_credentials(), sql_query, -1, dictionary=True)
        profile_resume_experience = {}
        for row in db_data:
            exp_years = int(row["Experience"])
            exp_months = int(round((row["Experience"] % 1) * 12, 0))
            profile_resume_experience[row["profile_id"]] = Utils.joinAtDecimal(exp_years, exp_months)
        return profile_resume_experience
