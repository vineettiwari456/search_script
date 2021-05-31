# -*-coding:utf-8 -*-
import mysql.connector
from mysql.connector import Error
from tqdm.notebook import tqdm
from math import ceil
from mysql.connector import Error, MySQLConnection, ProgrammingError
import time
from decouple import config

class DbUtils:
    def __init__(self):
        self.parser_conf={"user": config("PARSER_USER"), "password": config("PARSER_PASS"),"host":config("PARSER_HOST"),
                            "database": config("PARSER_DB")}
        self.falcon_read_conf = {"user": config("FALCON_R_USER"), "password": config("FALCON_R_PASS"),"host":config("FALCON_R_HOST"),
                            "database": config("FALCON_R_DB")}
        self.falcon_write_conf = {"user": config("FALCON_W_USER"), "password": config("FALCON_W_PASS"),"host":config("FALCON_W_HOST"),
                            "database": config("FALCON_W_DB")}
        self.dolphin_conf = {"user": config("DOLPHIN_USER"), "password": config("DOLPHIN_PASS"),"host":config("DOLPHIN_HOST"),
                            "database": config("DOLPHIN_DB")}
        self.dolphin_w_conf = {"user": config("DOLPHIN_W_USER"), "password": config("DOLPHIN_W_PASS"),
                             "host": config("DOLPHIN_HOST"),
                             "database": config("DOLPHIN_DB")}
    def get_parser_db(self):
        db = mysql.connector.connect(
            host=self.parser_conf["host"], user=self.parser_conf["user"], passwd=self.parser_conf["password"], db=self.parser_conf["database"]
        )
        return db

    def get_falcon_db(self):
        db = mysql.connector.connect(
            host=self.falcon_read_conf["host"], user=self.falcon_read_conf["user"], passwd=self.falcon_read_conf["password"], db=self.falcon_read_conf["database"]
        )
        return db

    def get_falcon_write_db(self):
        db = mysql.connector.connect(
            host=self.falcon_write_conf["host"], user=self.falcon_write_conf["user"], passwd=self.falcon_write_conf["password"], db=self.falcon_write_conf["database"]
        )
        return db

    def get_dolphin_db(self):
        db = mysql.connector.connect(
            host=self.dolphin_conf["host"], user=self.dolphin_conf["user"], passwd=self.dolphin_conf["password"], db=self.dolphin_conf["database"]
        )
        return db
    def get_dolphin_write_db(self):
        db = mysql.connector.connect(
            host=self.dolphin_w_conf["host"], user=self.dolphin_w_conf["user"], passwd=self.dolphin_w_conf["password"], db=self.dolphin_w_conf["database"]
        )
        return db

    def close(self,conn, cursor) -> None:
        cursor.close()
        conn.close()

    def my_query(self,
        conn,
        query,
        args = None,
        get = True,
        dictionary = False
    ):
        while True:
            try:
                cursor = conn.cursor(dictionary=dictionary)
                if args:
                    cursor.executemany(query, args)
                else:
                    cursor.execute(query)
                result = None
                if get:
                    result = cursor.fetchall()
                else:
                    conn.commit()
                    result = cursor.rowcount
                self.close(conn, cursor)
                return result
            except Error as err:
                if isinstance(err, ProgrammingError):
                    raise ValueError(f"Query failed '{query}'")
                if "try restarting transaction" in err.msg:
                    time.sleep(1)
                    continue
                if "Can't connect to MySQL server on" in err.msg:
                    for _ in tqdm(
                        range(30), desc="Waiting as unable to connect to MySQL"
                    ):
                        time.sleep(1)
                    continue
                raise ValueError(f"Query failed '{query}'.")

    def get_included(self,
        excluded, min_id, max_id
    ):
        included = []
        lo = min_id
        for begin, end in excluded:
            if begin > lo:
                included.append((lo, begin))
            lo = max(lo, end)
        if max_id > lo:
            included.append((lo, max_id))
        return included

    def partition(self,
        included, partition_size
    ):
        params = []
        total_ids = 0
        for begin, end in included:
            sz = end - begin
            partition_cnt = ceil(sz / partition_size)
            for i in range(partition_cnt):
                param_start = begin + i * partition_size
                param_end = min(begin + (i + 1) * partition_size, end)
                params.append((param_start, param_end))
        return params