import os

env = os.environ.get("env")
if env == "PROD":
    #AWS_ES_END_POINT = "https://search-monster-elastic-prod-tyurj6ygcxzrcrqqpfmjkgeroe.ap-south-1.es.amazonaws.com"
    AWS_ES_END_POINT = "https://search-monster-elastic-prod-rec-inejk4g4bnqvxua36dm56rbihq.ap-south-1.es.amazonaws.com"
    GRAYLOG_PORT = 6001
elif env == "QA":
    AWS_ES_END_POINT = "https://search-monster-elastic-uat-247dfxtqoiff7fvpumppqcwdui.ap-south-1.es.amazonaws.com"
    #AWS_ES_END_POINT = "http://10.216.240.63:9200"
    GRAYLOG_PORT = 6000
else:
    AWS_ES_END_POINT = "https://search-monster-elastic-nld3cnw6j7mrovkixykyaryrua.ap-south-1.es.amazonaws.com"
    # AWS_ES_END_POINT = "http://10.216.240.58:9200"
    AWS_ES_END_POINT="http://172.30.6.22:9200"
    GRAYLOG_PORT = 6000

SITE_CONTEXTS = [
    "rexmonster",
    "monstergulf",
    "monsterhongkong",
    "monstersingapore",
    "monsterphilippines",
    "monsterthailand",
    "monstervietnam",
    "monsterindonesia",
    "monstermalaysia",
]
OTHER_SPECIALIZATIONS = [
    "dcc5d316-8c74-4008-9855-0534046ab7ed",
    "f35398dd-f76b-406a-9864-2c5e6ff0cbd5",
    "0e5ff30c-c58c-4070-bd74-51541c41d5ca",
    "9ceb4e0c-7804-4ed8-900f-9ec965ac115f",
    "dc254767-43fd-443b-80ae-b628d39b2736",
    "898a0ad5-e87f-40c9-a9c8-21f88e6a3526",
    "000554e0-1a93-4b0e-ae5e-f046e3050c56",
    "738531e7-6e41-447b-b9a6-cce3b475bfd6",
    "5a457948-6406-4e29-bf9c-7fea121f9d35",
    "b0689956-02a2-45dd-83cc-346e84cbc6f4",
]
OTHER_LOCATIONS = [
    "7ad1a3d0-7b61-4d4a-9cb0-6dfc7c7c61f4",
    "1660969d-c9c0-4796-92c6-72341a169a01",
    "f87807e7-9afe-4683-83ac-c804349ef727",
    "3b2ef3d6-0f02-49f3-beef-4ba6c56f9e1b",
    "79bc4252-da28-46de-baf5-b727c942d68a",
    "11244a12-47ee-4c73-90b5-2d007e7d9e9c",
    "c738edd7-3da7-419b-90ab-a6316fe22ae1",
    "3728a895-d9ad-4ae3-871b-b97ea954db78",
    "a828983a-fe47-4539-90aa-f2d84f742a63",
    "60b4cd73-2678-4702-a767-eb68667cb0b0",
    "5c73ea49-1366-45f0-966a-ab2cd16d1262",
    "93e8dab3-002d-4291-a15f-b0e3ceb26977",
    "5bb53186-90bd-4f6b-bbf6-3f70dccc7811",
    "3b65f55e-38c8-47fa-954b-5104dbac1524",
    "eb43cfa7-62d1-4501-bf29-200fc55f7273",
    "beee6146-d8dc-49f3-85d9-640470d2c9b9",
    "ca5e49ec-e4ea-4183-b953-0faaf20956d0",
    "76390cd2-3b35-11e9-a89e-70106fbef856",
    "fbbed309-4636-11e9-a89e-70106fbef856",
]
OTHER_HIGHEST_QUALIFICATION = "d1f770d0-7701-4b97-9dfc-8963caf12bad"
OTHER_COLLEGE = "2efdca6a-7024-438f-8f47-33c2d6353f07"
YIN = "yin"
YANG = "yang"

SITE_CONTEXT_2_DEFAULT_CURRENCY = {
    "rexmonster": "INR",
    "monstergulf": "AED",
    "monsterhongkong": "HKD",
    "monstersingapore": "SGD",
    "monsterphilippines": "PHP",
    "monsterthailand": "THB",
    "monstervietnam": "VND",
    "monsterindonesia": "IDR",
    "monstermalaysia": "MYR"
}
