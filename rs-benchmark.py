#!/usr/bin/env python3

import os
import sys
import logging
import click
import psycopg2
import string
import pandas as pd

from time import time
from datetime import datetime
from getpass import getpass
from random import seed, choice, random, randint

logger = None
password = None
table_name = "_rs_benchmarking"

def _get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(choice(letters) for i in range(length))

def _init_logger(debug=False):
    global logger

    if not logger:
        # Initialise logger
        logger = logging.getLogger("rs-benchmark")
        logger.propagate = False # avoid printing messages multiple times
        if logger.handlers == []:
            ch = logging.StreamHandler(sys.stdout) # Set logging to stdout to avoid red backgroud text
            if debug:
                logger.setLevel(level=logging.DEBUG)
                ch.setLevel(level=logging.DEBUG)
            else:
                logger.setLevel(level=logging.INFO)
                ch.setLevel(level=logging.INFO)
            formatter = logging.Formatter(
                "%(levelname)s:\t%(message)s"
            )
            ch.setFormatter(formatter)
            logger.addHandler(ch)

def _connect_redshift(db_host, db_port, db_name, db_user):
    "Connects to Redshift and returns a psycopg2 connection object"
    logger.debug(f"Connect to {db_user}@{db_host}:{db_name}")
    try:
        global password

        if not password:
            # see if an environment variable exists
            password = os.environ.get("DB_PASSWORD", None)
            if not password:
                password = getpass(prompt="Type your password (or set DB_PASSWORD env variable): ")
                
        return psycopg2.connect(
            dbname=db_name,
            host=db_host,
            port=db_port,
            user=db_user,
            password=password,
            sslmode="require",
        )
    except Exception as e:
        logger.error(f"Unable to connect to Redshift: {str(e)}")
        return None

def _execute(conn, sql, data=None, auto_commit=True):
    conn.autocommit = auto_commit
    cur = conn.cursor()

    tic = time()
    if data:
        if type(data[0]) is tuple: # if we have tuple of tuples this will be a bulk insert
            # split the tuple, make sure it is not too large
            max_nbr_of_tuples = 50000
            for x in range(0, len(data), max_nbr_of_tuples):
                full_sql = sql + ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", d).decode("utf-8") for d in data[x:x+max_nbr_of_tuples])
                cur.execute(full_sql)
        else:
            cur.execute(sql, data)
    else:
        cur.execute(sql)
    toc = time()

    # return the time it took
    return toc - tic

def _init_table(conn):
    # first try to delete it
    t = 0

    try:
        sql = f"drop table {table_name};"
        t = t + _execute(conn=conn, sql=sql, auto_commit=True)
    except Exception as e:
        logger.debug(f"Exception occurred when trying to delete table, however ignore it: {e}")
        pass

    sql = f"""
        create temp table {table_name}(
            id bigint identity(0, 1),
            my_integer integer,
            my_smallint smallint,
            my_decimal decimal(8,2),
            my_timestamp timestamp,
            my_varchar varchar(100) 
        );
    """
    t = t + _execute(conn=conn, sql=sql, auto_commit=True)
    logger.info(f"Create table executed in {round(t/1000, 4)} seconds")

@click.group()
def main():
    pass

@main.command()
@click.option('-h', '--db-host', help='Server name', required=True)
@click.option('-p', '--db-port', help='Server port', default=5439, show_default=True)
@click.option('-D', '--db-name', help='Database name', required=True)
@click.option('-u', '--db-user', help='User name', required=True)
@click.option('-c', '--copy-s3-path', help='When testing copy scenario: the s3 path to use', required=False)
@click.option('-i', '--copy-iam-role', help='When testing copy scenario: the iam role to use', required=False)
@click.option('-n', '--nbr-of-records', help='Number of records to insert', default=10, show_default=True)
@click.option('-s', '--scenario', help='Which scenario(s) to execute', type=click.Choice(['all','classic', 'bulk', 'copy'], case_sensitive=False), default='all', show_default=True)
@click.option('-d', '--debug', help="Debug logging", is_flag=True, default=False)
def insert(db_host, db_port, db_name, db_user, copy_s3_path, copy_iam_role, nbr_of_records, scenario, debug):
    _init_logger(debug)
    conn = _connect_redshift(
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        )

    seed(1)

    if scenario in ['bulk', 'all']:
        # Bulk insert
        _init_table(conn)
        sql = f"""
        INSERT INTO {table_name}  (
            my_integer,
            my_smallint,
            my_decimal,
            my_timestamp,
            my_varchar
        ) VALUES
        """
        logger.debug(f"Insert {nbr_of_records} records with bulk insert")

        tic = time()
        all_data = ()
        for i in range(0, nbr_of_records):
            data = (
                randint(0, 1e9),
                randint(0, 1e3),
                random(),
                datetime.now(),
                _get_random_string(100),
            )
            all_data = all_data + (data,) # this must be a tuple of tuples

        sql_time = _execute(conn=conn, sql=sql, data=all_data)
        toc = time()
        click.secho(f"Bulk insert for {nbr_of_records} records: {round(toc - tic, 3)} of which {round(sql_time, 3)} in sql", bold=True)

    if scenario in ['copy', 'all']:
        # Copy insert

        if not (copy_iam_role and copy_s3_path):
            click.secho("copy_iam_role and copy_s3_path are required!", bg="red")
            sys.exit(1)

        columns  = ["my_integer", "my_smallint", "my_decimal", "my_timestamp", "my_varchar"]

        _init_table(conn)
        logger.debug(f"Insert {nbr_of_records} records with copy insert")

        tic = time()
        all_data = []
        for i in range(0, nbr_of_records):
            all_data.append(
                [
                    randint(0, 1e9),
                    randint(0, 1e3),
                    random(),
                    datetime.now(),
                    _get_random_string(100),
                ]
            )

        df_total = pd.DataFrame(all_data, columns=columns)
        output_file = f"{copy_s3_path.rstrip('/')}/_rs_benchmark/{datetime.now().strftime('%Y%m%d-%H:%M:%S')}.csv"

        df_total.to_csv(output_file, sep=";", index=False)

        sql = f"""
        COPY {table_name}
        FROM '{output_file}'
        IAM_ROLE '{copy_iam_role}'
        FORMAT AS CSV
        TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
        DELIMITER ';'
        IGNOREHEADER AS 1
        ;
        """

        sql_time = _execute(conn=conn, sql=sql)
        toc = time()
        click.secho(f"Copy insert for {nbr_of_records} records: {round(toc - tic, 3)} of which {round(sql_time, 3)} in sql", bold=True)

    if scenario in ['classic', 'all']:
        # Classic insert
        max_records = 100
        if nbr_of_records > max_records:
            click.secho(f"For classic inserts, max nbr of records is {max_records}, using that then.", bg="red")
            nbr_of_records = max_records

        _init_table(conn)
        sql = f"""
        INSERT INTO {table_name}  (
            my_integer,
            my_smallint,
            my_decimal,
            my_timestamp,
            my_varchar
        ) VALUES (%s, %s, %s, %s, %s);
        """
        logger.debug("Classic insert {nbr_of_records} records")
        # logger.debug(f"Using this statement:\n{sql}")

        tic = time()
        sql_time = 0
        for _ in range(1, nbr_of_records):
            data = (
                randint(0, 1e9),
                randint(0, 1e3),
                random(),
                datetime.now(),
                _get_random_string(100),
            )
            sql_time = sql_time + _execute(conn=conn, sql=sql, data=data)
        toc = time()
        click.secho(f"Classic insert for {nbr_of_records} records: {round(toc - tic, 3)} of which {round(sql_time, 3)} in sql", bold=True)

if __name__ == '__main__':
    try:
        main()
    finally:
        try:
            conn.close()
            logger.debug("Connection closed")
        except Exception:
            pass
