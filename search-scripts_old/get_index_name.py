import argparse
from utils.esutils import ESUtils

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--active", action='store_true')
    parser.add_argument("--inactive", action='store_true')

    argv = vars(parser.parse_args())

    es_conn = ESUtils.esConn()

    if argv.get("active"):
        print(ESUtils.get_active_inactive_indexes(es_conn, 'livecore')["active_index"])
    else:
        print(ESUtils.get_active_inactive_indexes(es_conn, 'livecore')["inactive_index"])
