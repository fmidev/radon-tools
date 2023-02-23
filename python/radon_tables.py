#!/usr/bin/env python3
#
# Create or drop tables to radon database based on the metadata in the database
#

import sys
import datetime
import optparse
import csv
import psycopg2
import psycopg2.extras
import re
import os
import shutil
import time
import logging
import math
import subprocess
import dotenv
import boto3
import botocore
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer

from pytz import timezone
from optparse import OptionParser


def GetEnv(key):
    # mask exception to None
    try:
        return os.environ[key]
    except KeyError as e:
        return None


def ReadCommandLine(argv):

    parser = OptionParser(usage="usage: %prog [options] filename", version="%prog 1.0")

    databasegroup = optparse.OptionGroup(parser, "Options for database connection")

    parser.add_option(
        "-r",
        "--producer_id",
        action="store",
        type="int",
        help="Producer id (optional), conflicts with -c",
    )

    parser.add_option(
        "-c",
        "--class_id",
        action="store",
        type="int",
        help="Producer class id (optional), conflicts with -r",
    )

    parser.add_option(
        "-g",
        "--geometry_id",
        action="store",
        type="int",
        help="geometry id (optional)",
    )

    parser.add_option(
        "-d",
        "--date",
        action="store",
        type="string",
        help="Date for which tables are created (YYYYMMDD)",
    )

    parser.add_option(
        "--log-level",
        action="store",
        type="int",
        help="log level: 1-5 where 1 is least logging",
    )

    parser.add_option(
        "--show-sql",
        action="store_true",
        default=False,
        help="Show SQL statements that are executed",
    )

    parser.add_option(
        "--dry-run",
        action="store_true",
        default=False,
        help="Do not make any changes to database, sets --show-sql",
    )

    parser.add_option(
        "--recreate-triggers",
        action="store_true",
        default=False,
        help="Recreate partition triggers only",
    )

    parser.add_option(
        "--drop",
        action="store_true",
        default=False,
        help="Drop table instead of creating them",
    )

    parser.add_option(
        "--unlink",
        action="store_true",
        default=False,
        help="Physically unlink removed files",
    )

    parser.add_option(
        "--s3-credentials-file",
        action="store",
        type="string",
        help="Full filename of S3 credentials file. Can be templated with {hostname} and {bucketname}. File consists of KEY=VALUE pairs",
    )

    databasegroup.add_option(
        "--host",
        action="store",
        type="string",
        help="Database hostname (also env variable RADON_HOSTNAME)",
    )

    databasegroup.add_option(
        "--port",
        action="store",
        type="string",
        help="Database port (also env variable RADON_PORT)",
    )

    databasegroup.add_option(
        "--database",
        action="store",
        type="string",
        help="Database name (also env variable RADON_DATABASENAME)",
    )

    databasegroup.add_option(
        "--user",
        action="store",
        type="string",
        default="radon_admin",
        help="Database username",
    )

    parser.add_option_group(databasegroup)

    dbhost = (
        GetEnv("RADON_HOSTNAME")
        if GetEnv("RADON_HOSTNAME") is not None
        else "radondb.fmi.fi"
    )
    dbport = GetEnv("RADON_PORT") if GetEnv("RADON_PORT") is not None else "5432"
    dbname = (
        GetEnv("RADON_DATABASENAME")
        if GetEnv("RADON_DATABASENAME") is not None
        else "radon"
    )

    parser.set_defaults(host=dbhost, port=dbport, database=dbname)

    (options, arguments) = parser.parse_args()

    # Check requirements

    if options.date != None and (
        not re.match("\d{8}", options.date) or len(options.date) != 8
    ):
        print("Invalid format for date: %s" % (options.date))
        print("Should be YYYYMMDD")
        sys.exit(1)

    if options.date != None and options.drop:
        print("Option date cannot be combined with option --drop")
        sys.exit(1)

    if options.class_id != None and options.producer_id != None:
        print("Both producer and class cannot be specified")
        sys.exit(1)

    if options.class_id == None and options.producer_id == None:
        print("Either -c or -r must be specified")
        parser.print_help()
        sys.exit(1)

    if options.class_id != None and options.class_id not in [1, 3, 4]:
        print("Invalid producer class: %d" % options.class_id)
        print("Allowed values are:")
        print("1 (grid)\n2 (obs-not supported currently)\n3 (previ)\n4 (analysis)")
        sys.exit(1)

    if options.log_level == 1:
        options.log_level = logging.CRITICAL
    elif options.log_level == 2:
        options.log_level = logging.ERROR
    elif options.log_level == 3:
        options.log_level = logging.WARNING
    elif options.log_level == 4:
        options.log_level = logging.INFO
    elif options.log_level == 5:
        options.log_level = logging.DEBUG
    else:
        options.log_level = logging.INFO

    if options.dry_run:
        options.show_sql = True

    return (vars(options), arguments)


def SourceEnv(options, hostname, bucketname):
    envfile = (
        options["s3_credentials_file"]
        .replace("{hostname}", hostname.strip("https://").strip("http://"))
        .replace("{bucketname}", bucketname)
    )

    if not os.path.isfile(envfile):
        logging.error(f"File {envfile} does not exist")
        return False

    dotenv.load_dotenv(dotenv_path=envfile, override=True)
    logging.debug(f"Sourced file {envfile}")
    return True


def CreateClient(hostname):

    session = boto3.session.Session()
    hostname_ = (
        f"https://{hostname}" if hostname.startswith("http") is False else hostname
    )
    return session.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=hostname_,
    )


def GeomIds(producer_id, class_id, partitioning_period=None):

    meta_table = "table_meta_grid"

    query = "SELECT distinct geometry_id FROM " + as_table + " WHERE producer_id = %s"
    args = (producer_id,)

    if partitioning_period != None:
        query += " AND partitioning_period = %s"
        args += (partitioning_period,)

    if options["show_sql"]:
        print("%s %s" % (query, args))

    cur.execute(query, args)

    rows = cur.fetchall()

    ret = []

    for row in rows:
        ret.append(row[0])

    return ret


def GridPartitionExists(options, producer, geometry, partition, analysis_time):
    query = "SELECT count(*) FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND partition_name = %s AND analysis_time = %s"

    if options["show_sql"]:
        print("%s %s" % (query, (producer, geometry, partition, analysis_time)))

    cur.execute(query, (producer, geometry, partition, analysis_time))

    row = cur.fetchone()

    return int(row[0]) == 1


def PreviPartitionExists(options, producer, partition):
    query = (
        "SELECT count(*) FROM as_previ WHERE producer_id = %s AND partition_name = %s"
    )

    if options["show_sql"]:
        print("%s %s" % (query, (producer, partition)))

    cur.execute(query, (producer, partition))

    row = cur.fetchone()

    return int(row[0]) == 1


def TableExists(options, schema_name, table_name):
    query = "SELECT count(*) FROM as_grid WHERE schema_name = %s AND table_name = %s"

    if options["show_sql"]:
        print("%s (%s)" % (query, (schema_name, table)))

    cur.execute(query, (schema_name, table_name))

    return int(cur.fetchone()[0]) >= 1


def GetProducer(producer_id):

    query = "SELECT id, name, class_id FROM fmi_producer WHERE id = %s"

    if options["show_sql"]:
        print("%s %s" % (query, (producer_id,)))

    cur.execute(query, (producer_id,))

    row = cur.fetchone()

    prod = {}
    prod["id"] = row[0]
    prod["name"] = row[1]
    prod["class_id"] = row[2]

    return prod


def GetProducersFromClass(class_id):

    query = "SELECT id, name, class_id FROM fmi_producer WHERE class_id = %s"

    if options["show_sql"]:
        print("%s %s" % (query, (class_id,)))

    cur.execute(query, (class_id,))

    ret = []

    for row in cur.fetchall():
        prod = {}
        prod["id"] = row[0]
        prod["name"] = row[1]
        prod["class_id"] = row[2]

        ret.append(prod)

    return ret


#
# Get table definitions defined in {table_meta_grid|table_meta_previ} based on the command
# line options.
#


def GetDefinitions(options):

    class_id = None

    if options["producer_id"] != None:
        producerinfo = GetProducer(options["producer_id"])

        if len(producerinfo) == 0:
            logging.critical("No producer metadata found with given options")
            sys.exit(1)

        class_id = producerinfo["class_id"]
    else:
        class_id = options["class_id"]

    query = ""

    if class_id == 1:
        query = "SELECT producer_id, table_name, retention_period, partitioning_period, analysis_times, schema_name, geometry_id FROM table_meta_grid"

    elif class_id == 3:
        query = "SELECT producer_id, table_name, retention_period, partitioning_period, analysis_times, schema_name FROM table_meta_previ"

    args = ()

    if options["producer_id"] != None:
        query += " WHERE producer_id = %s"
        args = (options["producer_id"],)

    if options["geometry_id"] != None:
        if class_id == 3:
            logging.error("geometry is not supported with previ producers")
        else:
            if options["producer_id"] == None:
                query += " WHERE geometry_id = %s"
            else:
                query += " AND geometry_id = %s"
            args = args + (options["geometry_id"],)

    query += " ORDER BY producer_id"

    if options["show_sql"]:
        print(
            "%s %s"
            % (
                query,
                args,
            )
        )

    cur.execute(query, args)

    rows = cur.fetchall()

    if len(rows) == 0:
        logging.warning("No definitions found from database with given options")

    ret = []

    for row in rows:
        definition = {}
        definition["producer_id"] = row[0]
        definition["table_name"] = row[1]
        definition["retention_period"] = row[2]
        definition["analysis_times"] = None if class_id in [2, 4] else row[4]
        definition["geometry_id"] = None if class_id in [2, 3] else row[6]
        definition["class_id"] = class_id
        definition["schema_name"] = row[5]
        definition["partitioning_period"] = row[3]

        ret.append(definition)

    return ret


def ListPartitions(options, producerinfo, table_name):

    as_table = "as_grid"

    if producerinfo["class_id"] == 3:
        as_table = "as_previ"

    query = (
        "SELECT table_name, partition_name FROM "
        + as_table
        + " WHERE table_name = %s  GROUP BY table_name, partition_name ORDER BY partition_name"
    )

    if options["show_sql"]:
        print("%s %s" % (query, (table_name,)))

    cur.execute(query, (table_name,))

    rows = cur.fetchall()

    partitions = []

    for row in rows:
        partitions.append(row[1])

    return partitions


def CreateMainTable(options, element, producerinfo):

    template_table = "grid_data_template"

    class_id = producerinfo["class_id"]

    if class_id == 3:
        template_table = "previ_data_template"

    query = "CREATE TABLE %s.%s (LIKE %s INCLUDING ALL)" % (
        element["schema_name"],
        element["table_name"],
        template_table,
    )

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        try:
            cur.execute(query)
            query = "GRANT SELECT ON %s.%s TO radon_ro" % (
                element["schema_name"],
                element["table_name"],
            )
            cur.execute(query)
            query = "GRANT INSERT,DELETE,UPDATE ON %s.%s TO radon_rw" % (
                element["schema_name"],
                element["table_name"],
            )
            cur.execute(query)

        except psycopg2.ProgrammingError as e:
            # Table existed already; this happened in dev but probably not in production
            if e.pgcode == "42P07":
                logging.debug("Table %s exists already" % (element["table_name"]))
                conn.rollback()  # "current transaction is aborted, commands ignored until end of transaction block"

            else:
                print(e)
                sys.exit(1)
    # Create foreign keys

    query = """
SELECT
    replace(tc.constraint_name, 'grid_data_template', '%s'),
    kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE
    tc.constraint_type = 'FOREIGN KEY'
    AND
    tc.table_schema = 'public'
    AND
    tc.table_name = '%s'
""" % (
        element["table_name"],
        template_table,
    )

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            query = (
                "ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s)"
                % (
                    element["schema_name"],
                    element["table_name"],
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                )
            )
            cur.execute(query)

    CreateViews(options, element, class_id)
    CreatePartitioningTrigger(options, producerinfo, element)


def CreatePartitioningTrigger(options, producerinfo, element):

    schema_name = element["schema_name"]
    table_name = element["table_name"]
    query = """
CREATE OR REPLACE FUNCTION %s_partitioning_f()
RETURNS TRIGGER AS $$
DECLARE
	analysistime varchar(12);
BEGIN
	analysistime := to_char(NEW.analysis_time, 'yyyymmddhh24mi');
""" % (
        table_name
    )

    partitions = ListPartitions(options, producerinfo, table_name)

    if len(partitions) == 0:
        query += "	RAISE EXCEPTION 'No partitions available';"
    else:
        first = True

        if element["partitioning_period"] == "ANALYSISTIME":
            for partition_name in partitions:
                analysis_time = partition_name.split("_")[2]

                ifelsif = "ELSIF"

                if first:
                    first = False
                    ifelsif = "IF"

                query += """
		%s analysistime = '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (
                    ifelsif,
                    analysis_time,
                    element["schema_name"],
                    partition_name,
                )

        else:
            for partition_name in partitions:
                period_start = None

                if element["partitioning_period"] == "ANNUAL":
                    period = partition_name[-4:]
                    period_start = datetime.datetime.strptime(period, "%Y")
                    delta = datetime.timedelta(years=1)

                elif element["partitioning_period"] == "MONTHLY":
                    period = partition_name[-6:]
                    period_start = datetime.datetime.strptime(period, "%Y%m")
                    delta = relativedelta(months=+1)

                elif element["partitioning_period"] == "DAILY":
                    period = partition_name[-8:]
                    period_start = datetime.datetime.strptime(period, "%Y%m%d")
                    delta = datetime.timedelta(days=1)

                period_stop = period_start + delta

                ifelsif = "ELSIF"

                if first:
                    first = False
                    ifelsif = "IF"

                query += """
		%s analysistime >= '%s' AND analysistime < '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (
                    ifelsif,
                    period_start.strftime("%Y-%m-%d"),
                    period_stop.strftime("%Y-%m-%d"),
                    element["schema_name"],
                    partition_name,
                )
        query += """
	ELSE
		RAISE EXCEPTION 'Partition not found for analysis_time %', NEW.analysis_time;
	END IF;"""

    query += """
	RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE"""

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        try:
            cur.execute(
                "LOCK %s.%s IN ACCESS EXCLUSIVE MODE" % (schema_name, table_name)
            )
            cur.execute(query)

        except psycopg2.extensions.TransactionRollbackError as e:
            logging.error(e)
            logging.info("sleeping for 20s and retrying...")
            time.sleep(20)

            cur.execute(
                "LOCK %s.%s IN ACCESS EXCLUSIVE MODE" % (schema_name, table_name)
            )
            cur.execute(query)

    query = "DROP TRIGGER IF EXISTS %s_partitioning_trg ON %s.%s" % (
        table_name,
        schema_name,
        table_name,
    )

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        cur.execute(query)

    query = (
        "CREATE TRIGGER %s_partitioning_trg BEFORE INSERT ON %s.%s FOR EACH ROW EXECUTE PROCEDURE %s_partitioning_f()"
        % (table_name, schema_name, table_name, table_name)
    )

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        cur.execute(query)


def DropFromSSState(options, producer, row):
    geometry_id = row[8]
    analysis_time = row[4]

    query = "DELETE FROM ss_state WHERE producer_id = %s AND geometry_id = %s AND analysis_time = %s"

    if options["show_sql"]:
        print(cur.mogrify(query, (producer["id"], geometry_id, analysis_time)))

    if not options["dry_run"]:
        try:
            cur.execute(query, (producer["id"], geometry_id, analysis_time))
        except psycopg2.ProgrammingError as e:
            logging.error(e)


def DropFromSSForecastStatus(options, producer, row):
    geometry_id = row[8]
    analysis_time = row[4]

    query = "DELETE FROM ss_forecast_status WHERE producer_id = %s AND (geometry_id = %s OR geometry_id IS NULL) AND analysis_time = %s"

    if options["show_sql"]:
        print(cur.mogrify(query, (producer["id"], geometry_id, analysis_time)))

    if not options["dry_run"]:
        try:
            cur.execute(query, (producer["id"], geometry_id, analysis_time))
        except psycopg2.ProgrammingError as e:
            logging.error(e)


def RemoveFiles(options, files):

    count = 0
    last_server = None
    client = None

    # disable boto3 internal logging
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)

    for row in files:
        filename = row[0]
        fileprotocol = int(row[1])
        if fileprotocol == 1:
            # normal file
            if not os.path.isfile(filename):
                logging.error("File %s does not exist" % (filename,))
                continue

            count += 1

            if not options["dry_run"]:
                try:
                    os.remove(filename)
                except OSError as e:
                    print(e)

            else:
                logging.debug(f"rm {filename}")

        elif fileprotocol == 2:
            # use boto3 library to delete files, because command line line tool 'aws' is
            # way too slow

            filename = filename[1:] if filename[0] == "/" else filename
            bucket = filename.split("/")[0]
            key = "/".join(filename.split("/")[1:])

            file_server = row[2]

            if (
                last_server != file_server
                and options["s3_credentials_file"] is not None
            ):

                if not SourceEnv(options, file_server, bucket):
                    continue

                last_server = file_server
                client = None

            if client is None:
                client = CreateClient(file_server)

            if not options["dry_run"]:
                try:
                    client.delete_object(Bucket=bucket, Key=key)
                    count += 1

                except botocore.exceptions.ClientError as e:
                    logging.error(
                        "File removal failed with code %s" % e.response["Error"]["Code"]
                    )

            else:
                logging.debug(f"rm s3://{bucket}/{key}")

    return count


def DropFromAsGrid(options, producer, row):
    schema_name = row[1]
    table_name = row[2]
    partition_name = row[3]
    analysis_time = row[4]
    age = row[7]
    geometry_id = row[8]

    logging.info(
        "Deleting from partition %s using (analysis time %s geometry %d) age is %s"
        % (partition_name, analysis_time, geometry_id, age)
    )

    files = []

    if options["unlink"]:
        query = f"SELECT file_location,file_protocol_id,file_server FROM {schema_name}.{partition_name} "
        query += "WHERE geometry_id = %s AND analysis_time = %s AND producer_id = %s GROUP BY 1,2,3 ORDER BY file_server"

        if options["show_sql"]:
            print(cur.mogrify(query, (geometry_id, analysis_time, producer["id"])))

        cur.execute(query, (geometry_id, analysis_time, producer["id"]))

        files = cur.fetchall()

        logging.info(f"Found {len(files)} distinct files to remove")

    # First delete rows in ss_state, then remove files in disk, finally drop table partition

    query = (
        "DELETE FROM "
        + schema_name
        + "."
        + partition_name
        + " WHERE producer_id = %s AND geometry_id = %s AND analysis_time = %s"
    )

    if options["show_sql"]:
        print(cur.mogrify(query, (producer["id"], geometry_id, analysis_time)))

    if not options["dry_run"]:
        try:
            cur.execute(query, (producer["id"], geometry_id, analysis_time))

        except psycopg2.ProgrammingError as e:
            logging.error(
                "Table %s.%s does not exist although listed in %s"
                % (schema_name, partition_name, as_table)
            )

    start = timer()
    count = RemoveFiles(options, files)
    stop = timer()

    if count > 0:
        logging.info("Removed %d files in %.1f seconds" % (count, (stop - start)))

    directories = []
    for f in files:
        if f[1] == 1:
            directories.append(os.path.dirname(f[0]))

    directories = list(set(directories))

    while len(directories) > 0:
        newdirectories = []
        for dirname in directories:
            if not os.path.isdir(dirname):
                logging.debug("Directory %s does not exist" % dirname)
                continue

            if (
                dirname == "/"
                or dirname == os.environ["MASALA_PROCESSED_DATA_BASE"]
                or dirname == os.environ["MASALA_RAW_DATA_BASE"]
            ):
                continue

            if not options["dry_run"]:
                try:
                    os.rmdir(dirname)
                    logging.info("Removed empty dir %s" % dirname)
                except OSError as e:
                    pass

            newdirectories.append(os.path.dirname(os.path.normpath(dirname)))
        directories = newdirectories


def GetTablesForProducer(options, producer):
    as_table = "as_grid"

    if producer["class_id"] == 3:
        as_table = "as_previ"

    query = ""

    args = (producer["id"],)

    query = "SELECT id, schema_name, table_name, partition_name, analysis_time, min_analysis_time, max_analysis_time, now()-delete_time"

    if as_table == "as_grid":
        query += ", geometry_id"

    query += " FROM " + as_table + " WHERE producer_id = %s AND delete_time < now()"

    if options["show_sql"]:
        print("%s, %s" % (query, args))

    cur.execute(query, args)

    return as_table, cur.fetchall()


def DropTables(options):

    producers = []

    if options["producer_id"] is not None:
        producers.append(GetProducer(options["producer_id"]))
    else:
        producers = GetProducersFromClass(options["class_id"])

    for producer in producers:

        as_table, rows = GetTablesForProducer(options, producer)

        if len(rows) == 0:
            logging.info("Producer: %d No tables expired" % (producer["id"]))
            continue

        logging.info("Producer: %d" % (producer["id"]))

        for row in rows:
            rowid = row[0]
            schema_name = row[1]
            table_name = row[2]
            partition_name = row[3]
            analysis_time = row[4]
            min_analysis_time = row[5]
            max_analysis_time = row[6]
            age = row[7]

            if as_table == "as_grid":
                DropFromAsGrid(options, producer, row)
                DropFromSSForecastStatus(options, producer, row)
                DropFromSSState(options, producer, row)

            elif as_table == "as_previ":
                logging.info(
                    "Deleting from partition %s using analysis time %s age %s"
                    % (partition_name, analysis_time, age)
                )

                query = (
                    "DELETE FROM "
                    + schema_name
                    + "."
                    + partition_name
                    + " WHERE producer_id = %s AND analysis_time BETWEEN %s AND %s"
                )
                args = (producer["id"], min_analysis_time, max_analysis_time)

                if options["show_sql"]:
                    print("%s, %s" % (query, args))

                if not options["dry_run"]:
                    cur.execute(query, args)

            query = "DELETE FROM " + as_table + " WHERE id = %s"
            args = (rowid,)

            if options["show_sql"]:
                print("%s, %s" % (query, args))

            if not options["dry_run"]:
                cur.execute(query, args)

            # If table partition is empty and it is not referenced in as_{grid|previ} anymore,
            # we can drop it

            query = "SELECT count(*) FROM " + as_table + " WHERE partition_name = %s"

            cur.execute(query, (partition_name,))

            row = cur.fetchone()

            if int(row[0]) == 0:
                query = "DROP TABLE %s.%s" % (schema_name, partition_name)

                if options["show_sql"]:
                    print(query)

                if not options["dry_run"]:
                    cur.execute(query)

            # fake options for this producer only
            opts = {}
            opts["producer_id"] = producer["id"]
            opts["geometry_id"] = None
            opts["show_sql"] = options["show_sql"]

            defs = GetDefinitions(opts)

            # Remove duplicate definitions resulting from different geometries (DB trigger doesn't consider geometries)
            defs2 = []
            for i in range(len(defs)):
                add = True
                for j in range(len(defs2)):
                    if (
                        defs[i]["schema_name"] == defs2[j]["schema_name"]
                        and defs[i]["table_name"] == defs2[j]["table_name"]
                        and defs[i]["analysis_times"] == defs2[j]["analysis_times"]
                    ):
                        add = False
                        break
                if add:
                    defs2.append(defs[i])

            defs = defs2

            for definition in defs:
                CreatePartitioningTrigger(options, producer, definition)

                if not options["dry_run"]:
                    conn.commit()

        if not options["dry_run"]:
            conn.commit()


def CreateViews(options, element, class_id):

    # Create a view on top of table

    if class_id == 1:
        query = """
CREATE OR REPLACE VIEW public.%s_v AS
SELECT
		a.producer_id,
		f.name AS producer_name,
		a.analysis_time,
		a.geometry_id,
		g.name AS geometry_name,
		a.param_id,
		p.name AS param_name,
		a.level_id,
		l.name AS level_name,
		a.level_value,
		a.level_value2,
		a.forecast_period,
		a.forecast_period + a.analysis_time AS forecast_time,
		a.file_location,
		a.file_server,
		a.forecast_type_id,
		t.name AS forecast_type_name,
		a.forecast_type_value,
		a.file_format_id,
		ff.name AS file_format_name,
		a.file_protocol_id,
		fp.name AS file_protocol_name,
		a.message_no,
		a.byte_offset,
		a.byte_length,
		a.last_updater,
		a.last_updated
FROM
		%s.%s a,
		fmi_producer f,
		level l,
		param p,
		geom g,
		forecast_type t,
		file_format ff,
		file_protocol fp
WHERE
		a.producer_id = f.id
		AND
		a.level_id = l.id
		AND
		a.param_id = p.id
		AND
		a.geometry_id = g.id
		AND
		ff.id = a.file_format_id
		AND
		fp.id = a.file_protocol_id
		AND
		a.forecast_type_id = t.id
""" % (
            element["table_name"],
            element["schema_name"],
            element["table_name"],
        )
    elif class_id == 3:
        query = """
CREATE OR REPLACE VIEW public.%s_v AS
SELECT
		a.producer_id,
		f.name AS producer_name,
		a.analysis_time,
		a.station_id,
		st_x(s.position) AS longitude,
		st_y(s.position) AS latitude,
		a.param_id,
		p.name AS param_name,
		a.level_id,
		l.name AS level_name,
		a.level_value,
		a.level_value2,
		a.forecast_period,
		a.forecast_period + a.analysis_time AS forecast_time,
		a.forecast_type_id,
		t.name AS forecast_type_name,
		a.forecast_type_value,
		a.value,
		a.last_updater,
		a.last_updated
FROM
		%s.%s a,
		fmi_producer f,
		level l,
		param p,
		forecast_type t,
		station s
WHERE
		a.producer_id = f.id
		AND
		a.level_id = l.id
		AND
		a.param_id = p.id
		AND 
		a.forecast_type_id = t.id
		AND
		s.id = a.station_id
		""" % (
            element["table_name"],
            element["schema_name"],
            element["table_name"],
        )

    if options["show_sql"]:
        print(query)

    if not options["dry_run"]:
        cur.execute(query)
        query = "GRANT SELECT ON public.%s_v TO public" % (element["table_name"])
        cur.execute(query)


def AddNewTableToAsGrid(options, element, analysis_time, partition_name):
    delete_time = analysis_time + element["retention_period"]

    query = "INSERT INTO as_grid (producer_id, analysis_time, geometry_id, delete_time, schema_name, table_name, partition_name, min_analysis_time, max_analysis_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    args = (
        element["producer_id"],
        analysis_time,
        element["geometry_id"],
        delete_time,
        element["schema_name"],
        element["table_name"],
        partition_name,
        analysis_time,
        analysis_time,
    )

    if options["show_sql"]:
        print("%s (%s)" % (query, args))

    if not options["dry_run"]:
        try:
            cur.execute("SAVEPOINT sp1")
            cur.execute(query, args)
            logging.info("Adding entry to as_grid for analysis time %s" % analysis_time)
        except psycopg2.IntegrityError:
            cur.execute("ROLLBACK TO SAVEPOINT sp1")
            logging.debug(
                "Table partition for analysis_time %s exists already" % analysis_time
            )
        else:
            cur.execute("RELEASE SAVEPOINT sp1")


def AddNewTableToAsPrevi(options, element, analysis_time, partition_name):
    delete_time = analysis_time + element["retention_period"]

    query = "INSERT INTO as_previ (producer_id, analysis_time, delete_time, schema_name, table_name, partition_name, min_analysis_time, max_analysis_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    args = (
        element["producer_id"],
        analysis_time,
        delete_time.strftime("%Y-%m-%d %H:%M:%S"),
        element["schema_name"],
        element["table_name"],
        partition_name,
        analysis_time,
        analysis_time,
    )

    if options["show_sql"]:
        print("%s (%s)" % (query, args))

    if not options["dry_run"]:
        cur.execute(query, args)


def CreateForecastPartition(options, element, producerinfo, analysis_time):
    # Determine partition length and name

    partition_name = None
    analysis_timestamp = datetime.datetime.strptime(analysis_time, "%Y%m%d%H%M")

    if element["partitioning_period"] == "ANALYSISTIME":
        partition_name = "%s_%s" % (element["table_name"], analysis_time)

    elif element["partitioning_period"] == "DAILY":
        period_start = datetime.datetime.strptime(date[0:8], "%Y%m%d")
        period_stop = period_start + datetime.timedelta(days=1)

        partition_name = "%s_%s" % (
            element["table_name"],
            period_start.strftime("%Y%m%d"),
        )

    elif element["partitioning_period"] == "MONTHLY":
        period_start = datetime.datetime.strptime(date[0:6], "%Y%m")
        period_stop = period_start + relativedelta(months=+1)

        partition_name = "%s_%s" % (
            element["table_name"],
            period_start.strftime("%Y%m"),
        )

    elif element["partitioning_period"] == "ANNUAL":
        period_start = datetime.datetime.strptime(date[0:4], "%Y")
        period_stop = period_start + datetime.timedelta(years=1)

        partition_name = "%s_%s" % (element["table_name"], period_start.strftime("%Y"))

    # Check if partition exists in as_grid
    # This is the case when multiple geometries share one table

    if producerinfo["class_id"] == 1:
        if GridPartitionExists(
            options,
            element["producer_id"],
            element["geometry_id"],
            partition_name,
            analysis_timestamp,
        ):
            logging.debug(
                "Table partition %s for geometry %s exists already"
                % (partition_name, element["geometry_id"])
            )
            return False

    elif producerinfo["class_id"] == 3:
        if PreviPartitionExists(options, element["producer_id"], partition_name):
            logging.debug("Table partition %s exists already" % (partition_name,))
            return False

    # Check that if as_grid did not have information on this partition, does the
    # table still exist. If so, then we just need to add a row to as_grid.

    query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

    if options["show_sql"]:
        print("%s %s" % (query, (element["schema_name"], partition_name)))

    if not options["dry_run"]:
        cur.execute(query, (element["schema_name"], partition_name))

    row = cur.fetchone()

    if row == None or int(row[0]) == 0:

        sql_timestamp = analysis_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        if element["partitioning_period"] == "ANALYSISTIME":
            logging.info("Creating partition %s" % (partition_name))
            query = (
                "CREATE TABLE %s.%s (CHECK (analysis_time = '%s')) INHERITS (%s.%s)"
                % (
                    element["schema_name"],
                    partition_name,
                    sql_timestamp,
                    element["schema_name"],
                    element["table_name"],
                )
            )
        else:
            logging.info(
                "Creating %s partition %s"
                % (element["partitioning_period"], partition_name)
            )
            query = (
                "CREATE TABLE %s.%s (CHECK (analysis_time >= '%s' AND analysis_time < '%s')) INHERITS (%s.%s)"
                % (
                    element["schema_name"],
                    partition_name,
                    period_start,
                    period_stop,
                    element["schema_name"],
                    element["table_name"],
                )
            )

        if options["show_sql"]:
            print(query)

        if not options["dry_run"]:
            cur.execute(query)
            query = "GRANT SELECT ON %s.%s TO radon_ro" % (
                element["schema_name"],
                partition_name,
            )
            cur.execute(query)
            query = "GRANT INSERT,DELETE,UPDATE ON %s.%s TO radon_rw" % (
                element["schema_name"],
                partition_name,
            )
            cur.execute(query)

        as_table = None

        if producerinfo["class_id"] == 1:
            as_table = "as_grid"
            query = (
                "ALTER TABLE %s.%s ADD CONSTRAINT %s_pkey PRIMARY KEY (producer_id, analysis_time, geometry_id, param_id, level_id, level_value, level_value2, forecast_period, forecast_type_id, forecast_type_value)"
                % (element["schema_name"], partition_name, partition_name)
            )

        elif producerinfo["class_id"] == 3:
            as_table = "as_previ"
            query = (
                "ALTER TABLE %s.%s ADD CONSTRAINT %s_pkey PRIMARY KEY (producer_id, analysis_time, station_id, param_id, level_id, level_value, level_value2, forecast_period, forecast_type_id, forecast_type_value)"
                % (element["schema_name"], partition_name, partition_name)
            )

        if options["show_sql"]:
            print(query)

        if not options["dry_run"]:
            cur.execute(query)

        query = (
            "CREATE TRIGGER %s_store_last_updated_trg BEFORE UPDATE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE store_last_updated_f()"
            % (partition_name, element["schema_name"], partition_name)
        )

        if options["show_sql"]:
            print(query)

        if not options["dry_run"]:
            cur.execute(query)

    if producerinfo["class_id"] == 1:
        AddNewTableToAsGrid(options, element, analysis_timestamp, partition_name)

    if producerinfo["class_id"] == 3:
        AddNewTableToAsPrevi(options, element, analysis_timestamp, partition_name)

    return True


def CreateTables(options, element, date):

    producerinfo = GetProducer(element["producer_id"])

    if producerinfo["class_id"] == 1:
        logging.info(
            "Producer: %s geometry: %s"
            % (element["producer_id"], element["geometry_id"])
        )
    else:
        logging.info("Producer: %d" % (element["producer_id"]))

    # Check that main table exists, both physically and in as_grid

    query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

    if options["show_sql"]:
        print("%s (%s)" % (query, (element["schema_name"], element["table_name"])))

    cur.execute(query, (element["schema_name"], element["table_name"]))

    if int(cur.fetchone()[0]) == 0:
        logging.debug(
            "Parent table %s.%s does not exists, creating"
            % (element["schema_name"], element["table_name"])
        )
        CreateMainTable(options, element, producerinfo)

    # All DDL on one producer is done in one transaction

    # If we don't add any partitions (meaning that they already exist)
    # there's no need to re-create the triggers

    partitionAdded = False

    if element["analysis_times"] is None:
        logging.critical("Analysis_time information from database is NULL")
        sys.exit(1)
    for atime in element["analysis_times"]:
        (minutes, hours) = math.modf(atime)
        hours = int(hours)
        minutes = int(round(minutes * 60))
        analysis_time = "%s%02d%02d" % (date, hours, minutes)
        if CreateForecastPartition(options, element, producerinfo, analysis_time):
            partitionAdded = True

    # Update trigger

    if partitionAdded:
        CreatePartitioningTrigger(options, producerinfo, element)

    if not options["dry_run"]:
        conn.commit()


def get_radon_version(cur):
    cur.execute("SELECT radon_version_f()")
    return int(cur.fetchone()[0])


if __name__ == "__main__":

    (options, files) = ReadCommandLine(sys.argv[1:])

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=options["log_level"],
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(
        "Connecting to database %s at host %s port %s"
        % (options["database"], options["host"], options["port"])
    )

    password = None

    try:
        password = os.environ["RADON_%s_PASSWORD" % (options["user"].upper())]
    except:
        logging.critical(
            "password should be given with env variable RADON_%s_PASSWORD"
            % (options["user"].upper())
        )
        sys.exit(1)

    dsn = "user=%s password=%s host=%s dbname=%s port=%s" % (
        options["user"],
        password,
        options["host"],
        options["database"],
        options["port"],
    )
    conn = psycopg2.connect(dsn)

    conn.autocommit = 0

    cur = conn.cursor()

    date = datetime.datetime.now().strftime("%Y%m%d")

    if options["date"] != None:
        date = options["date"]

    if options["drop"]:
        DropTables(options)
        sys.exit(0)

    definitions = GetDefinitions(options)

    for element in definitions:
        if options["recreate_triggers"]:
            logging.info("Recreating triggers for table %s" % (element["table_name"]))
            CreatePartitioningTrigger(
                options, GetProducer(element["producer_id"]), element
            )
            conn.commit()
        else:
            CreateTables(options, element, date)
