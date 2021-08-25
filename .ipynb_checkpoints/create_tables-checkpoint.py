import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    #config.read('song_dwh.cfg')
    config.read('dwh.cfg')
    KEY = config.get("AWS","key")
    SECRET = config.get("AWS","secret")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","dwh_cluster_identifier")
    DWH_DB = config.get("DWH","dwh_db")
    DWH_DB_USER = config.get("DWH","dwh_db_user")
    DWH_DB_PASSWORD = config.get("DWH","dwh_db_password")
    DWH_PORT = config.get("DWH","dwh_port")
    DWH_REGION = config.get("DWH","dwh_region")
    DWH_ENDPOINT = config.get("DWH", "dwh_endpoint")
    DWH_ENDPOINT = DWH_ENDPOINT.split(":")[0]

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT ,DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()