#!/usr/bin/env python
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
import bunch
import os
import shutil
from timeit import default_timer as timer

from dateutil.relativedelta import relativedelta

from pytz import timezone
from bunch import Bunch
from optparse import OptionParser

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0")

	databasegroup = optparse.OptionGroup(parser, "Options for database connection")

	parser.add_option("-r", "--producer_id",
					  action="store",
					  type="int",
					  help="Producer id (optional), conflicts with -c",)

	parser.add_option("-c", "--class_id",
					  action="store",
					  type="int",
					  help="Producer class id (optional), conflicts with -r",)

	parser.add_option("-g", "--geometry_id",
					  action="store",
					  type="int",
					  help="geometry id (optional)",)
	
	parser.add_option("-d", "--date",
					  action="store",
					  type="string",
					  help="Date for which tables are created (YYYYMMDD)",)

	parser.add_option("--show-sql",
					  action="store_true",
					  default=False,
					  help="Show SQL statements that are executed",)

	parser.add_option("--dry-run",
					  action="store_true",
					  default=False,
					  help="Do not make any changes to database, sets --show-sql",)

	parser.add_option("--validate",
					  action="store_true",
					  default=False,
					  help="Validate current configuration",)

	parser.add_option("--recreate-triggers",
					  action="store_true",
					  default=False,
					  help="Recreate partition triggers only",)

	parser.add_option("--drop",
					  action="store_true",
					  default=False,
					  help="Drop table instead of creating them",)

	parser.add_option("--unlink", 
					  action="store_true",
					  default=False,
					  help="Physically unlink removed files")

	databasegroup.add_option("--host",
					action="store",
					type="string",
					default="radondb.fmi.fi",
					help="Database hostname")

	databasegroup.add_option("--port",
					action="store",
					type="string",
					default="5432",
					help="Database port")

	databasegroup.add_option("--database",
					action="store",
					type="string",
					default="radon",
					help="Database name")

	databasegroup.add_option("--user",
					action="store",
					type="string",
					default="wetodb",
					help="Database username")

	parser.add_option_group(databasegroup)

	(options, arguments) = parser.parse_args()
  
	# Check requirements

	if options.date != None and (not re.match("\d{8}", options.date) or len(options.date) != 8):
		print "Invalid format for date: %s" % (options.date)
		print "Should be YYYYMMDD"
		sys.exit(1)

	if options.date != None and options.drop:
		print "Option date cannot be combined with option --drop"
		sys.exit(1)

	if options.class_id != None and options.producer_id != None:
		print "Both producer and class cannot be specified"
		sys.exit(1)

	if options.class_id == None and options.producer_id == None:
		print "Either -c or -r must be specified"
		parser.print_help()
		sys.exit(1)

	if options.class_id != None and options.class_id not in [1,3,4]:
		print "Invalid producer class: %d" % options.class_id
		print "Allowed values are:"
		print "1 (grid)\n2 (obs-not supported currently)\n3 (previ)\n4 (analysis)"
		sys.exit(1)

	if options.dry_run:
		options.show_sql = True

	return (options,arguments)


# Validate()
# Check that contents of as_grid and the database system tables are in sync

def Validate(options, date):

	definitions = GetDefinitions(options)

	# return value 0 --> OK
	# return value 1 --> NOT OK

	if len(definitions) == 0:
		return 1

	for element in definitions:

		producerinfo = GetProducer(element.producer_id)
		
		if producerinfo.class_id == 1:
			print "Producer: %d geometry: %d" % (element.producer_id, element.geometry_id)
		elif producerinfo.class_id == 3:
			print "Producer: %d" % (element.producer_id)

		# Check main table

		query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

		if options.show_sql:
			print "%s %s" % (query, (element.schema_name, element.table_name))

		cur.execute(query, (element.schema_name, element.table_name))
			
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table %s does not exist" % (element.table_name,)
			print "Fixing.."
			
			CreateMainTable(options, element, producerinfo)

		# Check main table view

		query = "SELECT count(*) FROM pg_views WHERE schemaname = 'public' AND viewname = %s"

		if options.show_sql:
			print "%s %s" % (query, (element.table_name + "_v",))

		cur.execute(query, (element.table_name + "_v",))

		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table view public.%s_v does not exist" % (element.table_name,)
			print "Fixing.."

			CreateViews(options, element, producerinfo.class_id)

		# Check that number of childs matches as_grid information

		as_table = 'as_grid'

		if producerinfo.class_id == 3:
			as_table = 'as_previ'

		query = "SELECT count(distinct partition_name) FROM " + as_table + " WHERE table_name = %s"

		if options.show_sql:
			print "%s %s" % (query, (element.table_name,))

		cur.execute(query, (element.table_name,))
		
		row = cur.fetchone()

		childs = int(row[0])

		query = "SELECT count(*) FROM pg_inherits i WHERE i.inhparent = '%s.%s'::regclass" % (element.schema_name,element.table_name)

		if options.show_sql:
			print "%s %s" % (query, (element.schema_name,element.table_name))

		cur.execute(query)
		
		row = cur.fetchone()
	
		if int(row[0]) != childs:
			print "Number of child tables for parent %s in %s (%d) does not match with database (%d)" % (element.table_name, as_table, childs, int(row[0]))
			return 1

		# Check that parent has partitioning trigger

		query = "SELECT count(*) FROM pg_trigger WHERE tgname = '%s_partitioning_trg'" % (element.table_name)

		if options.show_sql:
			print "%s %s" % (query, (element.table_name,))

		cur.execute(query)
		
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Table %s does not have partitioning trigger defined" % (element.table_name)
			print "Fixing.."

			CreatePartitioningTrigger(options, producerinfo, element)

		print "Parent table %s is valid" % (element.table_name)

		# Check tables for each analysis times

		for atime in element.analysis_times:
			analysis_time = "%s%02d" % (date,atime)
			partition_name = "%s_%s" % (element.table_name, analysis_time)

			# Check if partition is in as_grid

			args = ()
			if producerinfo.class_id == 1:
				query = "SELECT partition_name, record_count FROM " + as_table + " WHERE producer_id = %s AND geometry_id = %s AND table_name = %s AND partition_name = %s"
				args = (element.producer_id, element.geometry_id, element.table_name, partition_name)

			elif producerinfo.class_id == 3:
				query = "SELECT partition_name, record_count FROM " + as_table + " WHERE producer_id = %s AND table_name = %s AND partition_name = %s"
				args = (element.producer_id, element.table_name, partition_name)

			if options.show_sql: 
				print "%s %s" % (query, args)

			cur.execute(query, args)
			
			row = cur.fetchone()

			if row == None:
				print "Partition for analysis time %s (%s) for table %s is not in %s" % (analysis_time, partition_name, element.table_name, as_table)
				return 1

			record_count = row[1]

			query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

			if options.show_sql:
				print "%s %s" % (query, (element.schema_name, partition_name))

			cur.execute(query, (element.schema_name, partition_name))
			
			row = cur.fetchone()

			if int(row[0]) != 1:
				print "Partition %s is in %s but does not exist" % (element.table_name, as_table)
				return 1

			args = (analysis_time,)
			query = "SELECT count(*) FROM " + element.table_name + " WHERE analysis_time = to_timestamp(%s, 'yyyymmddhh24')"

			if producerinfo.class_id == 1:
				query += " AND geometry_id = %s"
				args = args + (element.geometry_id,)

			if options.show_sql:
				print "%s %s" % (query, args)

			cur.execute(query, args)
			
			row = cur.fetchone()

			if int(row[0]) != record_count:
				print "as_grid reports that record_count for table %s partition %s is %d, but database says it's %d" % (element.table_name, partition_name, record_count, int(row[0]))
				return 1

			if producerinfo.class_id == 1:
				print "Partition %s for analysis time %s geometry_id %d (table %s) is valid" % (partition_name, analysis_time, element.geometry_id, element.table_name)

			elif producerinfo.class_id == 3:
				print "Partition %s for analysis time %s (table %s) is valid" % (partition_name, analysis_time, element.table_name)

	return 0

def GeomIds(producer_id, class_id, partitioning_period = None):

	meta_table = "table_meta_grid"

	query = "SELECT distinct geometry_id FROM " + as_table + " WHERE producer_id = %s"
	args = (producer_id,)

	if partitioning_period != None:
		query += " AND partitioning_period = %s"
		args += (partitioning_period,)

	if options.show_sql:
		print "%s %s" % (query, args) 
		
	cur.execute(query, args)

	rows = cur.fetchall()

	ret = []

	for row in rows:
		ret.append(row[0])

	return ret

def GridPartitionExists(options, producer, geometry, partition):
	query = "SELECT count(*) FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND partition_name = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer, geometry, partition)) 
		
	cur.execute(query, (producer, geometry, partition))

	row = cur.fetchone()

	return (int(row[0]) == 1)

def PreviPartitionExists(options, producer, partition):
	query = "SELECT count(*) FROM as_previ WHERE producer_id = %s AND partition_name = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer, partition)) 
		
	cur.execute(query, (producer, partition))

	row = cur.fetchone()

	return (int(row[0]) == 1)

def TableExists(options, schema_name, table_name):
	query = "SELECT count(*) FROM as_grid WHERE schema_name = %s AND table_name = %s"

	if options.show_sql:
		print "%s (%s)" % (query, (schema_name, table))

	cur.execute(query, (schema_name, table_name))

	return (int(cur.fetchone()[0]) >= 1)


def GetProducer(producer_id):

	query = "SELECT id, name, class_id FROM fmi_producer WHERE id = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer_id,))

	cur.execute(query, (producer_id,))

	row = cur.fetchone()

	prod = Bunch()
	prod.id = row[0]
	prod.name = row[1]
	prod.class_id = row[2]

	return prod

def GetProducersFromClass(class_id):

	query = "SELECT id, name, class_id FROM fmi_producer WHERE class_id = %s"

	if options.show_sql:
		print "%s %s" % (query, (class_id,))

	cur.execute(query, (class_id,))

	ret = []

	for row in cur.fetchall():
		prod = Bunch()
		prod.id = row[0]
		prod.name = row[1]
		prod.class_id = row[2]

		ret.append(prod)

	return ret

#
# Get table definitions defined in {table_meta_grid|table_meta_previ} based on the command
# line options.
#

def GetDefinitions(options):

	class_id = None

	if options.producer_id != None:
		producerinfo = GetProducer(options.producer_id)

		if len(producerinfo) == 0:
			print "No producer metadata found with given options"
			sys.exit(1)

		class_id = producerinfo.class_id
	else:
		class_id = options.class_id

	query = ""

	if class_id == 1:
		query = "SELECT producer_id, table_name, retention_period, partitioning_period, analysis_times, schema_name, geometry_id FROM table_meta_grid"

	elif class_id == 3:
		query = "SELECT producer_id, table_name, retention_period, partitioning_period, analysis_times, schema_name FROM table_meta_previ"
	
	args = ()

	if options.producer_id != None:
		query += " WHERE producer_id = %s"
		args = (options.producer_id,)

	if options.geometry_id != None:
		if class_id == 3:
			print "geometry is not supported with previ producers"
		else:
			if options.producer_id == None:
				query += " WHERE geometry_id = %s"
			else:
				query += " AND geometry_id = %s"
			args = args + (options.geometry_id,)

	query += " ORDER BY producer_id"

	if options.show_sql:
		print "%s %s" % (query, args,)

	cur.execute(query, args)

	rows = cur.fetchall()

	if len(rows) == 0:
		print "No definitions found from database with given options"

	ret = []

	for row in rows:
		definition = Bunch()
		definition.producer_id = row[0]
		definition.table_name = row[1]
		definition.retention_period = row[2]
		definition.analysis_times = None if class_id in [2,4] else row[4]
		definition.geometry_id = None if class_id in [2,3] else row[6]
		definition.class_id = class_id
		definition.schema_name = row[5]
		definition.partitioning_period = row[3]

		ret.append(definition)
	
	return ret

def ListPartitions(options, producerinfo, table_name):

	as_table = "as_grid"

	if producerinfo.class_id == 3:
		as_table = "as_previ"

	query = "SELECT table_name, partition_name FROM " + as_table + " WHERE table_name = %s ORDER BY partition_name"

	if options.show_sql:
		print "%s %s" % (query, (table_name,))

	cur.execute(query, (table_name,))

	rows = cur.fetchall()

	partitions = []

	for row in rows:
		partitions.append(row[1])

	return partitions


def CreateMainTable(options, element, producerinfo):

	template_table = "grid_data_template"

	class_id = producerinfo.class_id

	if class_id == 3:
		template_table = "previ_data_template"

	query = "CREATE TABLE %s.%s (LIKE %s INCLUDING ALL)" % (element.schema_name, element.table_name, template_table)

	if options.show_sql:
		print query

	if not options.dry_run:
		try:
			cur.execute(query)
			query = "GRANT SELECT ON %s.%s TO radon_ro" % (element.schema_name, element.table_name)
			cur.execute(query)
			query = "GRANT INSERT,DELETE,UPDATE ON %s.%s TO radon_rw" % (element.schema_name, element.table_name)
			cur.execute(query)

		except psycopg2.ProgrammingError, e:
			# Table existed already; this happened in dev but probably not in production
			if e.pgcode == "42P07":
				print "Table %s exists already" % (element.table_name)
				conn.rollback() # "current transaction is aborted, commands ignored until end of transaction block"

			else:
				print e
				sys.exit(1)
	CreateViews(options, element, class_id)
	CreatePartitioningTrigger(options, producerinfo, element)

def CreatePartitioningTrigger(options, producerinfo, element):

	schema_name = element.schema_name
	table_name = element.table_name
	query = """
CREATE OR REPLACE FUNCTION %s_partitioning_f()
RETURNS TRIGGER AS $$
DECLARE
	analysistime varchar(10);
BEGIN
	analysistime := to_char(NEW.analysis_time, 'yyyymmddhh24');
""" % (table_name)

	partitions = ListPartitions(options, producerinfo, table_name)

	if len(partitions) == 0:
		query += "	RAISE EXCEPTION 'No partitions available';"
	else:
		first = True

		if element.partitioning_period == "ANALYSISTIME":
			for partition_name in partitions:
				analysis_time = partition_name[-10:]

				ifelsif = "ELSIF"

				if first:
					first = False
					ifelsif = "IF"

				query += """
		%s analysistime = '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (ifelsif, analysis_time, element.schema_name, partition_name)

		else:
			for partition_name in partitions:
				period_start = None


				if element.partitioning_period == 'ANNUAL':
					period = partition_name[-4:]
					period_start = datetime.datetime.strptime(period, '%Y')
					delta = relativedelta(years=+1)

				elif element.partitioning_period == 'MONTHLY':
					period = partition_name[-6:]
					period_start = datetime.datetime.strptime(period, '%Y%m')
					delta = relativedelta(months=+1)

				elif element.partitioning_period == 'DAILY':
					period = partition_name[-8:]
					period_start = datetime.datetime.strptime(period, '%Y%m%d')
					delta = relativedelta(days=+1)


				period_stop = period_start + delta

				ifelsif = "ELSIF"

				if first:
					first = False
					ifelsif = "IF"

				query += """
		%s analysistime >= '%s' AND analysistime < '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (ifelsif, period_start.strftime('%Y-%m-%d'), period_stop.strftime('%Y-%m-%d'), element.schema_name, partition_name)			
		query += """
	ELSE
		RAISE EXCEPTION 'Partition not found for analysis_time %', NEW.analysis_time;
	END IF;"""

	query += """
	RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE"""
	
	if options.show_sql:
		print query
	
	if not options.dry_run:
		try:
			cur.execute("LOCK %s.%s IN ACCESS EXCLUSIVE MODE" % (schema_name, table_name))
			cur.execute(query)

		except psycopg2.extensions.TransactionRollbackError,e:
			print e
			sleep(1)

			cur.execute("LOCK %s.%s IN ACCESS EXCLUSIVE MODE" % (schema_name, table_name))
			cur.execute(query)

	query = "DROP TRIGGER IF EXISTS %s_partitioning_trg ON %s.%s" % (table_name, schema_name, table_name)

	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

	query = "CREATE TRIGGER %s_partitioning_trg BEFORE INSERT ON %s.%s FOR EACH ROW EXECUTE PROCEDURE %s_partitioning_f()" % (table_name, schema_name, table_name, table_name)
	
	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

def UpdateSSState(options, producer, geometry_id, min_analysis_time, max_analysis_time):
	query = "DELETE FROM ss_state WHERE producer_id = %s AND geometry_id = %s AND analysis_time BETWEEN %s AND %s"

	if options.show_sql:
		print cur.mogrify(query, (producer.id, geometry_id, min_analysis_time, max_analysis_time))

	if not options.dry_run:
		try:
			cur.execute(query, (producer.id, geometry_id, min_analysis_time, max_analysis_time))
		except psycopg2.ProgrammingError,e:
			print e

def DropTables(options):

	producers = []

	if options.producer_id is not None:
		producers.append(GetProducer(options.producer_id))
	else:
		producers = GetProducersFromClass(options.class_id)

	for producer in producers:

		as_table = 'as_grid'

		if producer.class_id == 3:
			as_table = 'as_previ'

		query = ""
	
		args = (producer.id,)

		query = "SELECT id, schema_name, table_name, partition_name, min_analysis_time, max_analysis_time, now()-delete_time"

		if as_table == 'as_grid':
			query += ", geometry_id"

		query += " FROM "+ as_table + " WHERE producer_id = %s AND delete_time < now()"
		
		if options.show_sql:
			print "%s, %s" % (query, args)

		cur.execute(query, args)

		rows = cur.fetchall()

		if len(rows) == 0:
			print "Producer %d: No tables expired" % (producer.id)
			continue

		print "Producer: %d" % (producer.id)

		for row in rows:
			rowid = row[0]
			schema_name = row[1]
			table_name = row[2]
			partition_name = row[3]
			min_analysis_time = row[4]
			max_analysis_time = row[5]
			age = row[6]
			geometry_id = None if as_table == 'as_previ' else row[7]

			# strip microseconds off from timedelta
			print "Deleting partition %s with analysis times %s .. %s age %s" % (partition_name, min_analysis_time, max_analysis_time, age)

			if as_table == 'as_grid':
				directories = []

				if options.unlink:
					# Fetch file path with the last two directories removed; ie
					# /masala/data/forecasts/7_107/201809020600/KWBCONEDEG/150/FFG-MS_ground_0_ll_360_181_0_150_3_9.grib2
					# -->
					# /masala/data/forecasts/7_107/201809020600/KWBCONEDEG/
					# and then we can remove the whole directory without specifying individual files.
					# Note! If directory structure is changed on the righternmost end, this logic will fail
					query = "WITH x AS (SELECT regexp_split_to_array(file_location, '/') AS a FROM %s.%s " % (schema_name, partition_name)
					query += " WHERE geometry_id = %s AND analysis_time BETWEEN %s AND %s) SELECT "
					query += "distinct array_to_string(a[1:array_upper(a,1)-2],'/') FROM x"

					if options.show_sql:
						print cur.mogrify(query, (geometry_id, min_analysis_time, max_analysis_time))

					cur.execute(query, (geometry_id, min_analysis_time, max_analysis_time))

					directories = cur.fetchall()

				# First delete rows in ss_state, then remove files in disk, finally drop table partition

				query = "DELETE FROM " + schema_name + "." + partition_name + " WHERE producer_id = %s AND geometry_id = %s AND analysis_time BETWEEN %s AND %s"

				if options.show_sql:
					print cur.mogrify(query, (producer.id, geometry_id, min_analysis_time, max_analysis_time))

				if not options.dry_run:
					try:
						cur.execute(query, (producer.id, geometry_id, min_analysis_time, max_analysis_time))
						UpdateSSState(options, producer, geometry_id, min_analysis_time, max_analysis_time)

						conn.commit() # commit at this point as file delete might take a long
						              # time sometimes

					except psycopg2.ProgrammingError,e:
						print "Table %s.%s does not exist although listed in %s" % (schema_name,partition_name,as_table)

				for row in directories:
					directory = row[0]
					if not os.path.isdir(directory):
						print "Directory %s does not exist" % (directory)
						continue

					start = timer()

					if not options.dry_run:
						shutil.rmtree(directory, ignore_errors=True)

					stop = timer()
					print "Removed directory %s in %.2f seconds" % (directory, (stop-start))

					# check if parent dir is empty, and if so remove it
					parent_dir = '/'.join(directory.split('/')[:-1])

					try:
						if not os.listdir(parent_dir):
							print "Removing empty parent dir %s" % (parent_dir)
							shutil.rmtree(parent_dir, ignore_errors=True)
					except OSError,e:
						print e

			elif as_table == 'as_previ':
				query = "DELETE FROM " + schema_name + "." + partition_name + " WHERE producer_id = %s AND analysis_time BETWEEN %s AND %s"
				args = (producer.id, min_analysis_time, max_analysis_time)

				if options.show_sql:
					print "%s, %s" % (query, args)

				if not options.dry_run:
					cur.execute(query, args)

			query = "DELETE FROM " + as_table + " WHERE id = %s"
			args = (rowid,)
				
			if options.show_sql:
				print "%s, %s" % (query, args)

			if not options.dry_run:
				cur.execute(query, args)

			# If table partition is empty and it is not referenced in as_{grid|previ} anymore,
			# we can drop it

			query = "SELECT count(*) FROM " + as_table + " WHERE partition_name = %s"

			cur.execute(query, (partition_name,))

			row = cur.fetchone()

			if int(row[0]) == 0:
				query = "DROP TABLE %s.%s" % (schema_name, partition_name)

				if options.show_sql:
					print query

				if not options.dry_run:
					cur.execute(query)

			# fake options for this producer only
			opts =  Bunch()
			opts.producer_id = producer.id
			opts.geometry_id = None
			opts.show_sql = options.show_sql

			defs = GetDefinitions(opts)

			for definition in defs:
				CreatePartitioningTrigger(options, producer, definition)

				if not options.dry_run:
					conn.commit()

		if not options.dry_run:
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
		a.last_updater,
		a.last_updated
FROM
		%s.%s a,
		fmi_producer f,
		level l,
		param p,
		geom g,
		forecast_type t
WHERE
		a.producer_id = f.id
		AND
		a.level_id = l.id
		AND
		a.param_id = p.id
		AND
		a.geometry_id = g.id
		AND 
		a.forecast_type_id = t.id
""" % (element.table_name, element.schema_name, element.table_name)
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
		""" % (element.table_name, element.schema_name, element.table_name)

	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)
		query = "GRANT SELECT ON public.%s_v TO public" % (element.table_name)
		cur.execute(query)


def CreateForecastPartition(options, element, producerinfo, analysis_time):
	# Determine partition length and name

	partition_name = None
	period_start = None
	period_stop = None

	if element.partitioning_period == "ANALYSISTIME":
		period_start = datetime.datetime.strptime(analysis_time, '%Y%m%d%H')
		period_stop = datetime.datetime.strptime(analysis_time, '%Y%m%d%H')

		partition_name = "%s_%s" % (element.table_name, analysis_time)

	elif element.partitioning_period == 'DAILY':
		period_start = datetime.datetime.strptime(date[0:8], '%Y%m%d')
		period_stop = period_start + relativedelta(days=+1)
		
		partition_name = "%s_%s" % (element.table_name, period_start.strftime('%Y%m%d'))

	elif element.partitioning_period == 'MONTHLY':
		period_start = datetime.datetime.strptime(date[0:6], '%Y%m')
		period_stop = period_start + relativedelta(months=+1)
		
		partition_name = "%s_%s" % (element.table_name, period_start.strftime('%Y%m'))

	elif element.partitioning_period == 'ANNUAL':
		period_start = datetime.datetime.strptime(date[0:4], '%Y')
		period_stop = period_start + relativedelta(years=+1)

		partition_name = "%s_%s" % (element.table_name, period_start.strftime('%Y'))

	# Check if partition exists in as_grid
	# This is the case when multiple geometries share one table

	if producerinfo.class_id == 1:
		if GridPartitionExists(options, element.producer_id, element.geometry_id, partition_name):
			print "Table partition %s for geometry %s exists already" % (partition_name, element.geometry_id)
			return False
	elif producerinfo.class_id == 3:
		if PreviPartitionExists(options, element.producer_id, partition_name):
			print "Table partition %s exists already" % (partition_name,)
			return False

	# Check that if as_grid did not have information on this partition, does the
	# table still exist. If so, then we just need to add a row to as_grid.

	query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

	if options.show_sql:
		print "%s %s" % (query, (element.schema_name, partition_name))

	if not options.dry_run:
		cur.execute(query, (element.schema_name, partition_name))

	row = cur.fetchone()

	if row == None or int(row[0]) == 0:

		analysis_time_timestamp = datetime.datetime.strptime(analysis_time, '%Y%m%d%H').strftime('%Y-%m-%d %H:%M:%S')
		
		if element.partitioning_period == "ANALYSISTIME":
			print "Creating partition %s" % (partition_name)
			query = "CREATE TABLE %s.%s (CHECK (analysis_time = '%s')) INHERITS (%s.%s)" % (element.schema_name, partition_name, analysis_time_timestamp, element.schema_name, element.table_name)
		else:
			print "Creating %s partition %s" % (element.partitioning_period, partition_name)
			period_start_timestamp = period_start.strftime('%Y-%m-%d %H:%M:%S')
			period_stop_timestamp = period_stop.strftime('%Y-%m-%d %H:%M:%S')
			query = "CREATE TABLE %s.%s (CHECK (analysis_time >= '%s' AND analysis_time < '%s')) INHERITS (%s.%s)" % (element.schema_name, partition_name, period_start_timestamp, period_stop_timestamp, element.schema_name, element.table_name)

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query)
			query = "GRANT SELECT ON %s.%s TO radon_ro" % (element.schema_name, partition_name)
			cur.execute(query)
			query = "GRANT INSERT,DELETE,UPDATE ON %s.%s TO radon_rw" % (element.schema_name, partition_name)
			cur.execute(query)

		as_table = None

		if producerinfo.class_id == 1:
			as_table = 'as_grid'
			query = "ALTER TABLE %s.%s ADD CONSTRAINT %s_pkey PRIMARY KEY (producer_id, analysis_time, geometry_id, param_id, level_id, level_value, level_value2, forecast_period, forecast_type_id, forecast_type_value)" % (element.schema_name, partition_name, partition_name)

		elif producerinfo.class_id == 3:
			as_table = 'as_previ'
			query = "ALTER TABLE %s.%s ADD CONSTRAINT %s_pkey PRIMARY KEY (producer_id, analysis_time, station_id, param_id, level_id, level_value, level_value2, forecast_period, forecast_type_id, forecast_type_value)" % (element.schema_name, partition_name, partition_name)

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query)
					
		query = "CREATE TRIGGER %s_store_last_updated_trg BEFORE UPDATE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE store_last_updated_f()" % (partition_name, element.schema_name, partition_name)

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query)

	# Delete time type depends on partitioning type; for 'ANALYSISTIME' it's 
	# analysis_time + retention, for others its now + retention

	delete_time = None

	if element.partitioning_period == "ANALYSISTIME":
	        delete_time = datetime.datetime.strptime(analysis_time, '%Y%m%d%H') + element.retention_period
	else:
		delete_time = datetime.datetime.now() + element.retention_period	

	args = ()
	
	if producerinfo.class_id == 1:
		query = "INSERT INTO as_grid (producer_id, analysis_time, geometry_id, delete_time, schema_name, table_name, partition_name, min_analysis_time, max_analysis_time) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s, %s, %s, %s)"
		args = (element.producer_id, analysis_time, element.geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), element.schema_name, element.table_name, partition_name, period_start.strftime('%Y-%m-%d %H:%M:%S'), period_stop.strftime('%Y-%m-%d %H:%M:%S'))

	if producerinfo.class_id == 3:
		query = "INSERT INTO as_previ (producer_id, analysis_time, delete_time, schema_name, table_name, partition_name, min_analysis_time, max_analysis_time) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s, %s, %s)"
		args = (element.producer_id, analysis_time, delete_time.strftime('%Y-%m-%d %H:%M:%S'), element.schema_name, element.table_name, partition_name, period_start.strftime('%Y-%m-%d %H:%M:%S'), period_stop.strftime('%Y-%m-%d %H:%M:%S'))
	
	if options.show_sql:
		print "%s (%s)" % (query, args)

	if not options.dry_run:
		cur.execute(query, args)

	return True

def CreateTables(options, element, date):

	producerinfo = GetProducer(element.producer_id)

	if producerinfo.class_id == 1:
		print "Producer: %s geometry: %s" % (element.producer_id, element.geometry_id) 
	else:
		print "Producer: %d" % (element.producer_id)

	# Check that main table exists, both physically and in as_grid
	
	query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

	if options.show_sql:
		print "%s (%s)" % (query, (element.schema_name, element.table_name))

	cur.execute(query, (element.schema_name, element.table_name))

	if int(cur.fetchone()[0]) == 0:

		print "Parent table %s.%s does not exists, creating" % (element.schema_name, element.table_name)
		CreateMainTable(options, element, producerinfo)

	# All DDL on one producer is done in one transaction

	# If we don't add any partitions (meaning that they already exist)
	# there's no need to re-create the triggers

	partitionAdded = False

	if element.analysis_times is None:
		if CreateForecastPartition(options,element,producerinfo,date + "00"):

			partitionAdded = True
	else:
		for atime in element.analysis_times:

			analysis_time = "%s%02d" % (date,atime)

			if CreateForecastPartition(options,element,producerinfo,analysis_time):
				partitionAdded = True

	# Update trigger 

	if partitionAdded:
		CreatePartitioningTrigger(options, producerinfo, element)

	if not options.dry_run:
		conn.commit()

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])

	print "Connecting to database %s at host %s port %s" % (options.database, options.host, options.port)

	password = None

	try:
		password = os.environ["RADON_%s_PASSWORD" % (options.user.upper())]
	except:
		print "password should be given with env variable RADON_%s_PASSWORD" % (options.user.upper())
		sys.exit(1)

	dsn = "user=%s password=%s host=%s dbname=%s port=%s" % (options.user, password, options.host, options.database, options.port)
	conn = psycopg2.connect(dsn)

	conn.autocommit = 0

	cur = conn.cursor()

	date = datetime.datetime.now().strftime('%Y%m%d')

	if options.date != None:
		date = options.date

	if options.validate:
		sys.exit(Validate(options, date))

	if options.drop:
		DropTables(options)
		sys.exit(0)

	definitions = GetDefinitions(options)

	for element in definitions:
		if options.recreate_triggers:
			print "Recreating triggers for table %s" % (element.table_name)
			CreatePartitioningTrigger(options, GetProducer(element.producer_id), element)
			conn.commit()
		else:
			CreateTables(options, element, date)

