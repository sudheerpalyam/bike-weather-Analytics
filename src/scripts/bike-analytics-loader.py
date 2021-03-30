#!/usr/bin/env python

import os
import sys
import getopt
import gzip
import snowflake.connector.connection
import snowflake.connector.errors
from codecs import open

import configparser


dbname = "CITIBIKE"
stagename = "CITIBIKE.PUBLIC.CITIBIKE_TRIPS2"

wsnames = {
    'XSMALL': 'ZEXP_100_XS_TEST'
}

if len(sys.argv) != 4:
    print("Usage snow_zexp_100_runner.py sourceTable targetTable numOfRows; given  {}".format(
        len(sys.argv)))
    sys.exit(1)


def run(conf):
    """Create database, Creates target tables, Unload data, upload data,
       capture runtime
    """

    account = os.environ["SNOW_ACCOUNT"]
    region = os.environ["SNOW_REGION"]
    password = os.environ["SNOW_PWD"]
    user = os.environ["SNOW_USER"]

    try:
        ctx = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            timezone='UTC',
            region=region
        )

        runinit(ctx, conf)
        runload(ctx, conf)

    finally:
        if 'ctx' in locals():
            ctx.close()


def runinit(ctx, conf):
    cs = ctx.cursor()
    print("==> USE DATABASE {}".format(conf['bikedbname']))
    cs.execute("USE DATABASE {}".format(conf['bikedbname']))

    load_weather_data(ctx, conf['weatherdbname'])

    load_biketrips_data(ctx, conf['bikedbname'])

    create_db_schema(ctx, conf['bikedbname'])
    create_unload_format(ctx)
    create_internal_stage(ctx, stagename)
    # change the size of the warehouse if test data size is larger than 5 GB and you want faster export
    print("==> using warehouse {}".format(wsnames['XSMALL']))
    create_warehouse(ctx, 'XSMALL')
    export_data(ctx, stagename,
                sys.argv[1], sys.argv[3])


def runload(ctx, conf):
    for ws_size in wsnames.keys():
        loadtable(ctx, ws_size)


def loadtable(ctx, ws_size):
    create_warehouse(ctx, ws_size)
    load_data(ctx, stagename,
              sys.argv[1], sys.argv[2])
    get_load_time(ctx, ws_size)


def get_load_time(ctx, ws_size):
    """Creates table to capture loadtime"""
    ws_name = wsnames[ws_size]
    try:
        cs = ctx.cursor()
        cs.execute("""CREATE TABLE IF NOT EXISTS EXP_RS_{} AS
    SELECT '{}' AS SIZE,QUERY_ID,TOTAL_ELAPSED_TIME,EXECUTION_TIME,START_TIME
    from table(information_schema.QUERY_HISTORY_BY_WAREHOUSE('{}'))
    WHERE query_type='COPY' AND execution_status='SUCCESS'
    ORDER BY start_time DESC""".format(ws_name, ws_name, ws_name))

        print("==> load time result for: {}".format(ws_name))

    finally:
        cs.close()


def create_db_schema(ctx, db_name):
    """Creates DB and Schema for load run"""
    try:
        cs = ctx.cursor()
        cs.execute("""CREATE DATABASE IF NOT EXISTS  {}""".format(db_name))

        print("==> created database: {}".format(db_name))
    finally:
        cs.close()


def create_internal_stage(ctx, stage_name):
    """Creates internal stage"""
    try:
        cs = ctx.cursor()
        cs.execute(""" create or replace stage {}
        file_format = zexp100_csv_unload_format""".format(stage_name))

        print("==> created stage: {}".format(stage_name))
    finally:
        cs.close()


def create_unload_format(ctx):
    """Creates unload format for export"""
    try:
        cs = ctx.cursor()
        cs.execute("""CREATE OR replace file format zexp100_csv_unload_format
                  type = 'CSV'
                  field_delimiter = '|'""")
        print("==> created export format: zexp100_csv_unload_format")
    finally:
        cs.close()


def create_warehouse(ctx, ws_size):
    try:

        cs = ctx.cursor()
        ws_name = wsnames[ws_size]
        cs.execute("""
        CREATE WAREHOUSE IF NOT EXISTS {}
        WAREHOUSE_SIZE = '{}'
        AUTO_SUSPEND = 300 AUTO_RESUME = FALSE
        COMMENT = 'Z-Exp'
        """.format(ws_name, ws_size))

        try:
            cs.execute("alter warehouse {} resume".format(ws_name))
        except:
            print("==> warehouse {} already running...".format(ws_name))

        print("==> created warehouse: {}".format(ws_name))
    finally:
        cs.close()


def export_data(ctx, stage_name, source_table_name, rownum):
    try:
        cs = ctx.cursor()
        print("==> COPY INTO @{} FROM (SELECT * FROM  {} LIMIT {})".format(stage_name,
                                                                           source_table_name, rownum))
        cs.execute("COPY INTO @{} FROM (SELECT * FROM  {} LIMIT {})".format(
            stage_name, source_table_name, rownum))
        print("==> exported data into : {}".format(stage_name))
    finally:
        cs.close()


def load_data(ctx, stage_name, source_table_name, target_table_name):
    try:
        cs = ctx.cursor()
        cs.execute("create or replace TABLE {} LIKE {}".format(
            target_table_name, source_table_name))
        cs.execute("COPY INTO {} FROM @{}".format(
            target_table_name, stage_name))
        print("==> loaded data into : {}".format(target_table_name))
    finally:
        cs.close()


def load_weather_data(ctx, db_name):
    sqlfile = 'src/sql/weather.sql'
    with open(sqlfile, 'r', encoding='utf-8') as f:
        for cur in ctx.execute_stream(f):
            for ret in cur:
                print(ret)


def load_biketrips_data(ctx, db_name):
    sqlfile = 'src/sql/bike-trips.sql'
    with open(sqlfile, 'r', encoding='utf-8') as f:
        for cur in ctx.execute_stream(f):
            for ret in cur:
                print(ret)


if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('src/cfg/config.properties')
    conf = dict(config.items('snowflake'))
    print (conf)
    print (conf['snow_region'])

    run(conf)
