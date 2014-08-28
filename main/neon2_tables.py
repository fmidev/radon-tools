#!/usr/bin/env python
#
# Create or drop tables to neon2 database based on the metadata in the database
#

import sys
import datetime
import optparse
import csv
import psycopg2
import psycopg2.extras
import re
import bunch

from pytz import timezone
from bunch import Bunch
from optparse import OptionParser

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0")

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
		sys.exit(1)

	if options.class_id != None and options.class_id not in [1,3]:
		print "Invalid producer class: %d" % options.class_id
		print "Allowed values are:"
		print "1 (grid)\n2 (obs-not supported currently)\n3 (previ)"
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
			return 1

		# Check main table view

		query = "SELECT count(*) FROM pg_views WHERE schemaname = 'public' AND viewname = %s"

		if options.show_sql:
			print "%s %s" % (query, (element.table_name + "_v",))

		cur.execute(query, (element.table_name + "_v",))

		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table view public.%s_v does not exist" % (element.table_name,)
			return 1

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
			print "Number of child tables for parent %s in as_grid (%d) does not match with database (%d)" % (element.table_name, childs, int(row[0]))
			return 1

		# Check that parent has partitioning trigger

		query = "SELECT count(*) FROM pg_trigger WHERE tgname = '%s_partitioning_trg'" % (element.table_name)

		if options.show_sql:
			print "%s %s" % (query, (element.table_name,))

		cur.execute(query)
		
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Table %s does not have partitioning trigger defined" % (element.table_name)
			return 1

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
				print "Partition %s for table %s is not in %s" % (partition_name, element.table_name, as_table)
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

def PartitionExists(options, producer, geometry, partition):
	query = "SELECT count(*) FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND partition_name = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer, geometry, partition)) 
		
	cur.execute(query, (producer, geometry, partition))

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

	for row in cur.fetchone():
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
		query = "SELECT producer_id, table_name, retention_period, analysis_times, schema_name, geometry_id FROM table_meta_grid"

	elif class_id == 3:
		query = "SELECT producer_id, table_name, retention_period, analysis_times, schema_name FROM table_meta_previ"

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
		definition.analysis_times = row[3]
		definition.geometry_id = None if class_id in [2,3] else row[5]
		definition.class_id = class_id
		definition.schema_name = row[4]

		ret.append(definition)
	
	return ret

def ListPartitions(options, table_name):

	query = "SELECT table_name, partition_name FROM as_grid WHERE table_name = %s ORDER BY partition_name"

	if options.show_sql:
		print query

	cur.execute(query, (table_name,))

	rows = cur.fetchall()

	partitions = []

	for row in rows:
		partitions.append(row[1])

	return partitions


def CreateMainTable(options, element, class_id):

	template_table = "grid_data_template"

	if class_id == 3:
		template_table = "previ_data_template"

	query = "CREATE TABLE %s.%s (LIKE %s INCLUDING ALL)" % (element.schema_name, element.table_name, template_table)

	if options.show_sql:
		print query

	if not options.dry_run:
		try:
			cur.execute(query)
		except psycopg2.ProgrammingError, e:
			# Table existed already; this happened in dev but probably not in production
			if e.pgcode == "42P07":
				print "Table %s exists already" % (element.table_name)
				conn.rollback() # "current transaction is aborted, commands ignored until end of transaction block"

			else:
				print e
				sys.exit(1)

	# Create a view on top of table

	if class_id == 1:
		query = """
CREATE OR REPLACE VIEW public.%s_v AS
SELECT
		a.producer_id,
		p.name AS producer_name,
		a.analysis_time,
		a.geometry_id,
		g.name AS geometry_name,
		a.param_id,
		p.name AS param_name,
		a.level_id,
		l.name AS level_name,
		a.level_value,
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
		m.producer_id,
		p.name AS producer_name,
		a.analysis_time,
		m.param_id,
		p.name AS param_name,
		m.level_id,
		l.name AS level_name,
		m.level_value,
		a.forecast_period,
		a.forecast_period + a.analysis_time AS forecast_time,
		a.value,
		a.forecast_type_id,
		t.name AS forecast_type_name,
		a.last_updater,
		a.last_updated
FROM
		%s.%s a,
		fmi_producer f,
		level l,
		param p,
		previ_meta m,
		forecast_type t
WHERE
		a.previ_meta_id = m.id
		AND
		m.producer_id = f.id
		AND
		m.level_id = l.id
		AND
		m.param_id = p.id
		AND 
		a.forecast_type_id = t.id
		""" % (element.table_name, element.schema_name, element.table_name)

	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

def CreatePartitioningTrigger(options, schema_name, table_name):

	query = """
CREATE OR REPLACE FUNCTION %s_partitioning_f()
RETURNS TRIGGER AS $$
DECLARE
	analysistime varchar(10);
BEGIN
	analysistime := to_char(NEW.analysis_time, 'yyyymmddhh24');
""" % (table_name)

	partitions = ListPartitions(options, element.table_name)

	if len(partitions) == 0:
		query += "	RAISE EXCEPTION 'No partitions available';"
	else:
		first = True

		for partition_name in partitions:
			analysis_time = partition_name[-10:]

			ifelsif = "ELSIF"

			if first:
				first = False
				ifelsif = "IF"

			query += """
		%s analysistime = '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (ifelsif, analysis_time, element.schema_name, partition_name)

		query += """
	ELSE
		RAISE EXCEPTION 'Partition not found for analysis_time';
	END IF;"""

	query += """
	RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER VOLATILE"""
	
	if options.show_sql:
		print query
	
	if not options.dry_run:
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

def DropTables(options, element):

	producerinfo = GetProducer(element.producer_id)

	as_table = 'as_grid'

	if producerinfo.class_id == 3:
		as_table = 'as_previ'

	print "Producer: %d geometry: %d" % (element.producer_id, element.geometry_id)

	query = "SELECT analysis_time, partition_name, delete_time FROM as_grid WHERE producer_id = %s AND geometry_id = %s"

	cur.execute(query, (element.producer_id, element.geometry_id))

	rows = cur.fetchall()

	if len(rows) == 0:
		print "No tables"
		return

	current_time_naive = datetime.datetime.now()
	current_time = timezone('UTC').localize(current_time_naive)

	tablesDropped = False
	count = 0

	for row in rows:
		analysis_time = row[0]
		partition_name = row[1]
		delete_time = row[2]

		diff = delete_time - current_time

		if diff < datetime.timedelta(seconds=int(0)):
			# strip microseconds off from timedelta
			print "Deleting analysis time %s with age %s (partition %s)" % (analysis_time, str(diff).split('.')[0], partition_name)
			count = count+1

			# First delete contents
			
			if as_table == 'as_grid':
				query = "SELECT file_location FROM %s " % (partition_name,)
				query += " WHERE geometry_id = %s AND analysis_time = %s"

				if options.show_sql:
					print "%s %s" % (query, (element.geometry_id, analysis_time))

				rows = cur.execute(query, (element.geometry_id, analysis_time,))

				if rows != None:
					for row in cur.fetchone():
						file = row[0]

						if not os.path.isfile(file):
							print "File %s does not exist" % (file)
							continue

						os.remove(file)

			query = "DELETE FROM " + element.table_name + " WHERE producer_id = %s AND analysis_time = %s"

			args = (element.producer_id, analysis_time)

			if producerinfo.class_id == 1:
				query += " AND geometry_id = %s"
				args = args + (element.geometry_id,)

			if options.show_sql:
				print "%s, %s" % (query, args)

			if not options.dry_run:
				cur.execute(query, args)

			query = "DELETE FROM " + as_table + " WHERE producer_id = %s AND analysis_time = %s AND table_name = %s"
			args = (element.producer_id, analysis_time, element.table_name)
				
			if producerinfo.class_id == 1:
				query += " AND geometry_id = %s"
				args = args + (element.geometry_id,)

			if options.show_sql:
				print "%s, %s" % (query, args)

			if not options.dry_run:
				cur.execute(query, args)

			# If table partition is empty and it is not referenced in as_{grid|previ} anymore,
			# we can drop it

			query = "SELECT count(*) FROM as_grid WHERE partition_name = %s"

			cur.execute(query, (partition_name,))

			row = cur.fetchone()

			if int(row[0]) == 0:
				query = "DROP TABLE %s.%s" % (element.schema_name, partition_name)

				if options.show_sql:
					print query

				if not options.dry_run:
					cur.execute(query)

		else:
			print "Partition %s lifetime left %s" % (partition_name, str(diff).split('.')[0])

	if tablesDropped:
		CreatePartitioningTrigger(options, element.schema_name, element.table_name)

	if not options.dry_run:
		conn.commit()


def CreateTables(options, element, date):

	producerinfo = GetProducer(element.producer_id)

	if producerinfo.class_id == 1:
		print "Producer: %d geometry: %d" % (element.producer_id, element.geometry_id) 
	else:
	        print "Producer: %d" % (element.producer_id)

	# Check that main table exists, both physically and in as_grid
	
	query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

	if options.show_sql:
		print "%s (%s)" % (query, (element.schema_name, element.table_name))

	cur.execute(query, (element.schema_name, element.table_name))

	if int(cur.fetchone()[0]) == 0:

		print "Parent table %s.%s does not exists, creating" % (element.schema_name, element.table_name)
		CreateMainTable(options, element, producerinfo.class_id)

	# All DDL on one producer is done in one transaction

	# If we don't add any partitions (meaning that they already exist)
	# there's no need to re-create the triggers

	partitionAdded = False

	for atime in element.analysis_times:

		analysis_time = "%s%02d" % (date,atime)
		delete_time = datetime.datetime.strptime(analysis_time, '%Y%m%d%H') + element.retention_period

		partition_name = "%s_%s" % (element.table_name, analysis_time)
	
		# Check if partition exists in as_grid
		# This is the case when multiple geometries share one table

		if PartitionExists(options, element.producer_id, element.geometry_id, partition_name):
			print "Table partition %s for geometry %s exists already" % (partition_name, element.geometry_id)
			continue

		# Check that if as_grid did not have information on this partition, does the
		# table still exist. If so, then we just need to add a row to as_grid.

		query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

		if options.show_sql:
			print "%s %s" % (query, (element.schema_name, partition_name))

		if not options.dry_run:
			cur.execute(query, (element.schema_name, partition_name))

		row = cur.fetchone()

		if int(row[0]) == 0:
			partitionAdded = True
			print "Creating partition %s" % (partition_name)

			query = "CREATE TABLE %s.%s (CHECK (date_trunc('hour', analysis_time) = to_timestamp('%s', 'yyyymmddhh24'))) INHERITS (%s.%s)" % (element.schema_name, partition_name, analysis_time, element.schema_name, element.table_name)

			if options.show_sql:
				print query

			if not options.dry_run:
				cur.execute(query)

			as_table = 'as_grid'

			if producerinfo.class_id == 3:
				as_table = 'as_previ'

			query = "CREATE TRIGGER %s_update_as_table_trg BEFORE INSERT OR DELETE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE update_record_count_f('%s')" % (partition_name, element.schema_name, partition_name, as_table)

			if options.show_sql:
				print query

			if not options.dry_run:
				cur.execute(query)

		args = ()
		
		if producerinfo.class_id == 1:
			query = "INSERT INTO as_grid (producer_id, analysis_time, geometry_id, delete_time, schema_name, table_name, partition_name) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s, %s)"
			args = (element.producer_id, analysis_time, element.geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), element.schema_name, element.table_name, partition_name)

		if producerinfo.class_id == 3:
			query = "INSERT INTO as_previ (producer_id, analysis_time, delete_time, schema_name, table_name, partition_name) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s)"
			args = (element.producer_id, analysis_time, delete_time.strftime('%Y-%m-%d %H:%M:%S'), element.schema_name, element.table_name, partition_name)
		
		if options.show_sql:
			print "%s (%s)" % (query, args)

		if not options.dry_run:
			cur.execute(query, args)


	# Update trigger 

	if partitionAdded:
		CreatePartitioningTrigger(options, element.schema_name, element.table_name)

	if not options.dry_run:
		conn.commit()

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])

	username = 'wetodb'
	password = '3loHRgdio'
	hostname = 'dbdev.fmi.fi'
	port = 5432

	print "Connecting to database neon2 at host %s port %d" % (hostname, port)

	dsn = "user=%s password=%s host=%s dbname=neon2 port=%d" % (username, password, hostname, port)
	conn = psycopg2.connect(dsn)

	conn.autocommit = 0

	cur = conn.cursor()

	date = datetime.datetime.now().strftime('%Y%m%d')

	if options.date != None:
		date = options.date

	if options.validate:
		sys.exit(Validate(options, date))

	definitions = GetDefinitions(options)

	for element in definitions:
		if options.recreate_triggers:
			print "Recreating triggers for table %s" % (element.table_name)
			CreatePartitioningTrigger(options, element.schema_name, element.tablename)
			conn.commit()
		elif options.drop:
			DropTables(options, element)
		else:
			CreateTables(options, element, date)

