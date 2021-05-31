from collections import defaultdict

class CommonUtils:

    def appendUserIdsClauseForProfiles(sql_query, user_ids, user_range, profile_range, with_semicolon=True):
        if user_ids:
            sql_query += " and user_id in %s" % user_ids
        elif user_range:
            sql_query += " and user_id >= %d and user_id < %d" % (user_range[0], user_range[1])
        elif profile_range:
            sql_query += " and id >= %d and id < %d" % (profile_range[0], profile_range[1])
        else:
            raise Exception("Params not set")
        if with_semicolon:
            sql_query += ";"
        return sql_query

    def appendProfileIdsClause(sql_query, profile_ids=None, user_range=None, profile_range=None, with_semicolon=True):
        if profile_ids:
            sql_query += " and profile_id in %s" % profile_ids
        elif user_range:
            sql_query += " and profile_id in (select id from user_profiles where user_id >= %d and user_id < %d)" % (user_range[0], user_range[1])
        elif profile_range:
            sql_query += " and profile_id >= %d and profile_id < %d " % (profile_range[0], profile_range[1]) 
        else:
            raise Exception("Params not set")
        if with_semicolon:
            sql_query += ";"
        return sql_query

    def getSiteContextsVisibility(profile_ids, map_dict, profile_2_site_context, profile_preferred_locations):
        location_context_dict = map_dict["location_context"]
        profile_site_contexts_visibility = {}
        for profile_id in profile_ids:
            temp = defaultdict(lambda: 0)
            if profile_2_site_context.get(profile_id):
                temp[profile_2_site_context.get(profile_id)] += 1
            preferred_locations = profile_preferred_locations.get(profile_id, [])
            for preferred_location in preferred_locations:
                if location_context_dict.get(preferred_location["uuid"]):
                    temp[location_context_dict[preferred_location["uuid"]]] += 1
            profile_site_contexts_visibility[profile_id] = list(temp.keys())
        return profile_site_contexts_visibility
