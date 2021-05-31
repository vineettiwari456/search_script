import mysql.connector


class DBCredentials:

    @staticmethod
    def get_credentials():
        db_credentials = {'falcon': DBCredentials.falcon_credentials(), 'rio': DBCredentials.rio_credentials(),
                          'bazooka1': DBCredentials.bazooka1_credentials(),
                          'bazooka2': DBCredentials.bazooka2_credentials(),
                          'bazooka3': DBCredentials.bazooka3_credentials(),
                          'parser_db': DBCredentials.parser_db_credentials()}

        return db_credentials

    @staticmethod
    def falcon_credentials():
        return [
            {"host": "10.216.247.108", "user": "sandeep.euler", "password": "sandeep123", "database": "falcon"},
            {"host": "10.216.247.156", "user": "sandeep.euler", "password": "sandeep123", "database": "falcon"},
            {"host": "172.30.10.10", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "falcon"}
        ]

    @staticmethod
    def rio_credentials():
        return [
            {"host": "10.216.247.114", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "rio"},
            {"host": "172.30.10.52", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "rio"}
        ]

    @staticmethod
    def bazooka1_credentials():
        return [
            {"host": "172.30.10.20", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"},
            {"host": "172.30.10.22", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"}
        ]

    @staticmethod
    def bazooka2_credentials():
        return [
            {"host": "172.30.10.13", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"},
            {"host": "172.30.10.29", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"}
        ]

    @staticmethod
    def bazooka3_credentials():
        return [
            {"host": "172.30.10.43", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"},
            {"host": "172.30.10.44", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "bazooka"}
        ]

    @staticmethod
    def parser_db_credentials():
        return [
            {"host": "172.30.6.4", "user": "fulltimeindex", "password": "FuLLtIm#indEX@123#", "database": "euler"}
        ]
