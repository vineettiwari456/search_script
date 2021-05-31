import traceback
from contextlib import closing
import random
import mysql.connector
from .dbcredentials import DBCredentials

DBCredentials = DBCredentials.get_credentials()


class DBUtils:

    @staticmethod
    def mysql_connection(host, user, password, database):
        # print("Trying to connect to host: %s" % host)
        for i in [0, 1, 2]:
            try:
                return mysql.connector.connect(host=host, user=user, password=password, database=database)
            except:
                print("MySQL connection failed! Retyring %d.." % i)
                if i == 2:
                    print(traceback.format_exc())
                    print("MySQL connection failed 3 times. Giving up..")
                    raise

    @staticmethod
    def falcon_connection():
        credentials = DBCredentials['falcon'][random.randint(0, 2)]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def rio_connection():
        credentials = DBCredentials['rio'][random.randint(0, 1)]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def bazooka1_connection():
        credentials = DBCredentials['bazooka1'][random.randint(0, 1)]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def bazooka2_connection():
        credentials = DBCredentials['bazooka2'][random.randint(0, 1)]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def bazooka3_connection():
        credentials = DBCredentials['bazooka3'][random.randint(0, 1)]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def parser_db_credentials():
        credentials = DBCredentials['parser_db'][0]
        return DBUtils.mysql_connection(credentials['host'], credentials['user'], credentials['password'],
                                        credentials['database'])

    @staticmethod
    def fetch_results_in_batch(connection, query, batch_size, dictionary=False):
        rows = []
        with closing(connection.cursor(buffered=True, dictionary=dictionary)) as cursor:
            cursor.execute(query)
            if batch_size == -1:
                # print("fetching all columns")
                rows = cursor.fetchall()
            else:
                while True:
                    batch_empty = True
                    for row in cursor.fetchmany(batch_size):
                        batch_empty = False
                        rows.append(row)
                    if batch_empty:
                        break
        connection.close()
        return rows
