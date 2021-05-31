import mysql.connector
from .dbutils import DBUtils


class MasterDictionary:

    def getMapData():
        location_query = "select l1.uuid, l2.name from job_locations l1, job_location_langs l2 where l1.id=l2.job_location_id ;"
        gender_query = "select g1.uuid, g2.name from genders g1, gender_langs g2 where g1.id=g2.gender_id ;"
        nationality_query = "select n1.uuid, n2.name, n1.country from nationalities n1, nationality_langs n2 where n1.id=n2.nationality_id ;"
        skill_query = "select s1.uuid, s2.name from skills s1, skill_langs s2 where s1.id=s2.skill_id ;"
        company_query = "select c1.uuid, c2.name from search_companies c1, search_company_langs c2 where c1.id=c2.search_company_id ;"
        designation_query = "select d1.uuid, d2.name from designations d1, designation_langs d2 where d1.id=d2.designation_id ;"
        industry_query = "select i1.uuid, i2.name from industries i1, industry_langs i2 where i1.id=i2.industry_id ;"
        job_type_query = "select j1.uuid, j2.name from job_types j1, job_type_langs j2 where j1.id=j2.job_type_id ;"
        employment_type_query = "select e1.uuid, e2.name from employment_types e1, employment_type_langs e2 where e1.id=e2.employment_type_id ;"
        role_query = "select r1.uuid, r2.name from function_and_roles r1, function_and_role_langs r2 where r1.id=r2.function_and_role_id ;"
        qualification_query = "select q1.uuid, q2.name from highest_qualifications q1, highest_qualification_langs q2 where q1.id=q2.highest_qualification_id ;"
        specialization_query = "select s1.uuid, s2.name from qualification_specializations s1, qualification_specialization_langs s2 where s1.id=s2.qualification_specialization_id ;"
        college_query = "select c1.uuid, c2.name from colleges c1, college_langs c2 where c1.id=c2.college_id ;"
        language_query = "select l.uuid, l.name from languages l ;"
        marital_status_query = "select m1.uuid, m2.name from marital_status m1, marital_status_langs m2 where m1.id = m2.marital_status_id ;"
        category_query = "select c1.uuid, c2.name from reservation_categories c1, reservation_category_langs c2 where c1.id  = c2.reservation_category_id ;"
        function_and_role_query = "select r1.uuid , r2.uuid from function_and_roles r1, function_and_roles r2 where r1.parent_id = r2.id ;"
        salary_mode_query = "select s1.uuid , s2.name from salary_modes s1, salary_mode_langs s2 where s1.id = s2.salary_mode_id ;"
        conversion_to_usd_query = "select c.currency_code as code, c.usd_price from falcon.currencies c ;"
        channel_site_query = "select c.site_context , c.channel_id from falcon.channel_site_context c ;"
        sub_channel_site_query = "select c.site_context , c.kiwi_sub_channel_id from falcon.channel_site_context c ;"
        visa_type_query = "select v1.uuid , v2.name from visa_types v1, visa_type_langs v2 where v1.id = v2.visa_type_id ;"
        country_query = "select c1.uuid , c2.name, c1.iso_code from countries c1, country_langs c2 where c1.id = c2.country_id ;"
        it_skill_query = "select s1.uuid , s3.name from skills s1, skill_skill_category s2, skill_categories s3 where s1.id = s2.skill_id and s2.skill_category_id = s3.id and s3.name ='IT'; "
        location_context_query = "select uuid, site_context from job_locations;"
        disability_detail_query = "select uuid, name from disability_details dd, disability_detail_langs ddl where dd.id=ddl.disability_detail_id;"
        disability_type_query = "select uuid, name from disabilities, disability_langs where disabilities.id=disability_langs.disability_id;"
        map_data = {}

        map_data['locations'] = MasterDictionary.getMapping(location_query, DBUtils.falcon_connection())
        map_data['country_mapping'] = MasterDictionary.getCountryForCityMapping()
        map_data['gender'] = MasterDictionary.getMapping(gender_query, DBUtils.falcon_connection())
        map_data['nationality'] = MasterDictionary.getMappingDict(nationality_query, DBUtils.falcon_connection(),
                                                                  ['name', 'code'])
        map_data['skills'] = MasterDictionary.getMapping(skill_query, DBUtils.falcon_connection())
        map_data['company'] = MasterDictionary.getMapping(company_query, DBUtils.falcon_connection())
        map_data['designation'] = MasterDictionary.getMapping(designation_query, DBUtils.falcon_connection())
        map_data['industries'] = MasterDictionary.getMapping(industry_query, DBUtils.falcon_connection())
        map_data['job_type'] = MasterDictionary.getMapping(job_type_query, DBUtils.falcon_connection())
        map_data['employment_type'] = MasterDictionary.getMapping(employment_type_query, DBUtils.falcon_connection())
        map_data['roles'] = MasterDictionary.getMapping(role_query, DBUtils.falcon_connection())
        map_data['qualification'] = MasterDictionary.getMapping(qualification_query, DBUtils.falcon_connection())
        map_data['specialization'] = MasterDictionary.getMapping(specialization_query, DBUtils.falcon_connection())
        map_data['college'] = MasterDictionary.getMapping(college_query, DBUtils.falcon_connection())
        map_data['languages'] = MasterDictionary.getMapping(language_query, DBUtils.falcon_connection())
        map_data['marital_status'] = MasterDictionary.getMapping(marital_status_query, DBUtils.falcon_connection())
        map_data['category'] = MasterDictionary.getMapping(category_query, DBUtils.falcon_connection())
        map_data['function_and_role'] = MasterDictionary.getMappingUuid(function_and_role_query,
                                                                        DBUtils.falcon_connection())
        map_data['salary_mode'] = MasterDictionary.getMapping(salary_mode_query, DBUtils.falcon_connection())
        map_data['currency_conversion'] = MasterDictionary.getMappingUuid(conversion_to_usd_query,
                                                                          DBUtils.falcon_connection())
        map_data['channel_site'] = MasterDictionary.getMappingUuid(channel_site_query, DBUtils.falcon_connection())
        map_data['sub_channel_site'] = MasterDictionary.getMappingUuid(sub_channel_site_query,
                                                                       DBUtils.falcon_connection())
        map_data['visa_type'] = MasterDictionary.getMapping(visa_type_query, DBUtils.falcon_connection())
        map_data['country'] = MasterDictionary.getMappingDict(country_query, DBUtils.falcon_connection(),
                                                              ['name', 'code'])
        map_data['it_skill'] = MasterDictionary.getMapping(it_skill_query, DBUtils.falcon_connection())
        map_data['location_context'] = MasterDictionary.getMapping(location_context_query, DBUtils.falcon_connection())
        map_data['disability_detail'] = MasterDictionary.getMapping(disability_detail_query,
                                                                    DBUtils.falcon_connection())
        map_data['disability_type'] = MasterDictionary.getMapping(disability_type_query, DBUtils.falcon_connection())
        return map_data

    def getMappingUuid(query, connection):
        data = DBUtils.fetch_results_in_batch(connection, query, -1)
        mapping = {}
        for row in data:
            mapping[str(row[0])] = row[1]
        return mapping

    def getMapping(query, connection):
        data = DBUtils.fetch_results_in_batch(connection, query, -1)
        mapping = {}
        for row in data:
            if row[1] is not None:
                mapping[str(row[0])] = row[1].strip()
            else:
                mapping[str(row[0])] = None
        return mapping

    def getMappingDict(query, connection, fields):
        data = DBUtils.fetch_results_in_batch(connection, query, -1)
        mapping = {}
        for row in data:
            adict = {}
            i = 1
            for field in fields:
                adict[field] = row[i].strip()
                i = i + 1
            mapping[str(row[0])] = adict

        return mapping

    def getCountryForCityMapping():
        city_country_query = "select id, uuid, parent_id , location_type from " \
                             "job_locations where country != 'IN';"
        data = DBUtils.fetch_results_in_batch(DBUtils.falcon_connection(),
                                              city_country_query, -1, dictionary=True)
        parent_mapping = {}
        uuid_mapping = {}
        type_mapping = {}
        for row in data:
            parent_mapping[row["id"]] = row["parent_id"]
            uuid_mapping[row["id"]] = row["uuid"]
            type_mapping[row["id"]] = row["location_type"]
        country_mapping = {}
        for loc_id in uuid_mapping.keys():
            parent_loc_id = loc_id
            if type_mapping[parent_loc_id] != "COUNTRY":
                visited = {}
                loc_type = type_mapping.get(parent_loc_id)
                while parent_mapping[parent_loc_id] is not None and loc_type is not \
                        None and loc_type != "COUNTRY":
                    if visited.get(parent_loc_id):
                        break
                    visited[parent_loc_id] = True
                    parent_loc_id = parent_mapping[parent_loc_id]
                    loc_type = type_mapping.get(parent_loc_id)
            if uuid_mapping.get(loc_id) and uuid_mapping.get(parent_loc_id):
                country_mapping[uuid_mapping[loc_id]] = uuid_mapping.get(parent_loc_id)

        return country_mapping
