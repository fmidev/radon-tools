#
# Create or drop tables to neon2 database based on the metadata in the database
#

import sys
import datetime
import optparse
import csv
import psycopg2
import re
from pytz import timezone

from optparse import OptionParser

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0")

	parser.add_option("-r", "--producer",
					  action="store",
					  type="int",
					  help="Producer id (optional)",)

	parser.add_option("-g", "--geometry",
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

	if options.dry_run:
		options.show_sql = True

	return (options,arguments)


# Validate()
# Check that contents of as_grid and the database system tables are in sync

def Validate(options, date):

	tables = GetTables(options)

	# return value 0 --> OK
	# return value 1 --> NOT OK

	if len(tables) == 0:
		print "No records found for producer %d, geometry %d" % (options.producer, options.geometry)
		return 1

	for element in tables:
		producer = element[0]
		table_name = element[1]
		geometry = element[4]
		schema = element[5]

		print "Producer: %s geometry: %d" % (producer, geometry)

		# Check main table

		query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

		if options.show_sql:
			print "%s %s" % (query, (schema, table_name))

		cur.execute(query, (schema, table_name))
			
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table %s does not exist" % (table_name,)
			return 1

		# Check main table view

		query = "SELECT count(*) FROM pg_views WHERE schemaname = 'public' AND viewname = %s"

		if options.show_sql:
			print "%s %s" % (query, (table_name + "_v",))

		cur.execute(query, (table_name + "_v",))

		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table view public.%s_v does not exist" % (table_name,)
			return 1

		# Check that number of childs matches as_grid information

		query = "SELECT count(*) FROM as_grid WHERE table_name = %s"

		if options.show_sql:
			print "%s %s" % (query, (table_name,))

		cur.execute(query, (table_name,))
		
		row = cur.fetchone()

		childs = int(row[0])

		query = "SELECT count(*) FROM pg_inherits i WHERE i.inhparent = '%s.%s'::regclass" % (schema,table_name)

		if options.show_sql:
			print "%s" % (query,)

		cur.execute(query)
		
		row = cur.fetchone()
	
		if int(row[0]) != childs:
			print "Number of child tables for parent %s in as_grid (%d) does not match with database (%d)" % (table_name, childs, int(row[0]))
			return 1

		# Check that parent has partitioning trigger

		query = "SELECT count(*) FROM pg_trigger WHERE tgname = '%s_partitioning_trg'" % (table_name)

		if options.show_sql:
			print "%s" % (query)

		cur.execute(query)
		
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Table %s does not have partitioning trigger defined" % (table_name)
			return 1

		print "Parent table %s is valid" % (table_name)

		# Check tables for each analysis times

		for atime in element[3]:
			analysis_time = "%s%02d" % (date,atime)
			partition = "%s_%s" % (table_name, analysis_time)

			# Check if partition is in as_grid

			query = "SELECT partition_name, record_count FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND table_name = %s AND partition_name = %s"

			if options.show_sql: 
				print "%s %s" % (query, (producer, geometry,table_name, partition))

			cur.execute(query, (producer, geometry,table_name, partition))
			
			row = cur.fetchone()

			if row == None:
				print "Partition %s for table %s is not in as_grid" % (partition, table_name)
				return 1

			record_count = row[1]

			query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

			if options.show_sql:
				print "%s %s" % (query, (schema, partition))

			cur.execute(query, (partition,))
			
			row = cur.fetchone()

			if int(row[0]) != 1:
				print "Partition %s is in as_grid but does not exist" % (table_name,)
				return 1

			query = "SELECT count(*) FROM " + table_name + " WHERE geometry_id = %s"

			if options.show_sql:
				print "%s %s" % (query, (geometry,))

			cur.execute(query, (geometry,))
			
			row = cur.fetchone()

			if int(row[0]) != record_count:
				print "as_grid reports that record_count for table %s partition %s is %d, but database says it's %d" % (table_name, partition, record_count, int(row[0]))
				return 1

			print "Partition %s for geometry %d (table %s) is valid" % (partition, geometry, table_name)

	return 0

def PartitionExists(options, producer, geometry, partition):
	query = "SELECT count(*) FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND partition_name = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer, geometry, partition)) 
		
	cur.execute(query, (producer, geometry, partition))

	row = cur.fetchone()

	return (int(row[0]) == 1)

def TableExists(options, schema, table):
	query = "SELECT count(*) FROM as_grid WHERE schema = %s AND table_name = %s"

	if options.show_sql:
		print "%s (%s)" % (query, (schema, table))

	cur.execute(query, (schema, table))

	return (int(cur.fetchone()[0]) >= 1)


def GetTables(options):

	producer = options.producer
	geometry = options.geometry

	query = "SELECT producer_id, table_name, retention_period, analysis_times, geometry_id, schema FROM table_meta"

	args = ()

	if producer != None:
		query += " WHERE producer_id = %s"
		args = (producer,)

	if geometry != None:
		if producer == None:
			query += " WHERE geometry_id = %s"
		else:
			query += " AND geometry_id = %s"
		args = args + (geometry,)

	query += " ORDER BY producer_id"

	if options.show_sql:
		print "%s %s" % (query, args,)

	cur.execute(query, args)

	rows = cur.fetchall()

	return rows

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

def CreateTriggers(options, schema, table_name):

	query = """
CREATE OR REPLACE FUNCTION %s_partitioning_f()
RETURNS TRIGGER AS $$
DECLARE
	analysistime varchar(10);
BEGIN
	analysistime := to_char(NEW.analysis_time, 'yyyymmddhh24');
""" % (table_name)

	partitions = ListPartitions(options, table_name)

	if len(partitions) == 0:
		query += "	RAISE EXCEPTION 'No partitions available';"
	else:
		first = True

		for partition in partitions:
			analysis_time = partition[-10:]

			ifelsif = "ELSIF"

			if first:
				first = False
				ifelsif = "IF"

			query += """
		%s analysistime = '%s' THEN
			INSERT INTO %s.%s VALUES (NEW.*);""" % (ifelsif, analysis_time, schema, partition)

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

	query = "DROP TRIGGER IF EXISTS %s_partitioning_trg ON %s.%s" % (table_name, schema, table_name)

	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

	query = "CREATE TRIGGER %s_partitioning_trg BEFORE INSERT ON %s.%s FOR EACH ROW EXECUTE PROCEDURE %s_partitioning_f()" % (table_name, schema, table_name, table_name)
	
	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

def DropTables(options, element):

	producer_id = element[0]
	table_name = element[1]
	geometry_id = element[4]
	schema = element[5]

	print "Producer: %d geometry: %d" % (producer_id, geometry_id)

	query = "SELECT analysis_time, partition_name, delete_time FROM as_grid WHERE producer_id = %s AND geometry_id = %s"

	cur.execute(query, (producer_id, geometry_id))

	rows = cur.fetchall()

	if len(rows) == 0:
		print "No tables"
		return

	current_time_naive = datetime.datetime.now()
	current_time = timezone('UTC').localize(current_time_naive)

	tablesDropped = False

	for row in rows:
		analysis_time = row[0]
		partition = row[1]
		delete_time = row[2]

		diff = delete_time - current_time

		# strip microseconds off

		if diff > datetime.timedelta(seconds=int(0)):
			print "Deleting analysis time %s with age %s (partition %s)" % (analysis_time, str(diff).split('.')[0], partition)

			# First unlink all files

			query = "SELECT file_location FROM %s WHERE geometry_id = %s" % (partition, geometry_id)

			if options.show_sql:
				print query

			rows = cur.execute(query)

			if rows != None:
				for row in cur.fetchone():
					file = row[0]

					if not os.path.isfile(file):
						print "File %s does not exist" % (file)
						continue

					os.remove(file)

			query = "DELETE FROM " + table_name + " WHERE producer_id = %s AND geometry_id = %s AND analysis_time = %s"

			if options.show_sql:
				print "%s, %s" % (query, (producer_id, geometry_id, analysis_time))

			if not options.dry_run:
				cur.execute(query, (producer_id, geometry_id, analysis_time))

			# Check if the table is empty

			query = "SELECT count(*) FROM " + schema + "." + table_name + " WHERE analysis_time = %s"

			if options.show_sql:
				print "%s, %s" % (query, (analysis_time,))

			cur.execute(query, (analysis_time,))

			row = cur.fetchone()

			if int(row[0]) == 0:
				tablesDropped = True

				query = "DELETE FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND analysis_time = %s AND table_name = %s"

				if options.show_sql:
					print "%s, %s" % (query, (producer_id, geometry_id, analysis_time, table_name))

				if not options.dry_run:
					cur.execute(query, (producer_id, geometry_id, analysis_time, table_name))
			else:
				print "Partition %s still has some data (%d rows)" % (partition, int(row[0]))

			query = "SELECT count(*) FROM as_grid WHERE partition_name = %s"

			cur.execute(query, (partition,))

			row = cur.fetchone()

			if int(row[0]) == 0:
				query = "DROP TABLE %s.%s" % (schema, partition)

				if options.show_sql:
					print query

				if not options.dry_run:
					cur.execute(query)

		else:
			print "Partition %s lifetime left %s" % (partition, str(diff).split('.')[0])

	if tablesDropped:
		CreateTriggers(options, schema, table_name)

	if not options.dry_run:
		conn.commit()


def CreateTables(options, element, date):

	producer_id = element[0]
	table_name = element[1]
	retention_period = element[2]
	atimes = element[3]
	geometry_id = element[4]
	
	schema = element[5]

	print "Producer: %d geometry: %d" % (producer_id, geometry_id) 

	# Check that main table exists, both physically and in as_grid
	
	query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

if options.show_sql:
		print "%s (%s)" % (query, (schema, table_name))

	cur.execute(query, (schema, table_name))

	if int(cur.fetchone()[0]) == 0:

		print "Parent table %s.%s does not exists, creating" % (schema, table_name)

		query = "CREATE TABLE %s.%s (LIKE grid_data_template INCLUDING ALL)" % (schema, table_name)

		if options.show_sql:
			print query

		if not options.dry_run:
			try:
				cur.execute(query)
			except psycopg2.ProgrammingError, e:
				# Table existed already
				if e.pgcode == "42P07":
					print "Table %s exists already" % (table_name)
					conn.rollback() # "current transaction is aborted, commands ignored until end of transaction block"

				else:
					print e
					sys.exit(1)

	   # Create a view on top of table
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
		a.forecast_type_value
FROM
		%s.%s a,
		fmi_producer f,
		level l,
		param p,
		geom g
WHERE
		a.producer_id = f.id
		AND
		a.level_id = l.id
		AND
		a.param_id = p.id
		AND
		a.geometry_id = g.id
""" % (table_name, schema, table_name)

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query)


	# All DDL on one producer is done in one transaction

	# If we don't add any partitions (meaning that they already exist)
	# there's no need to re-create the triggers

	partitionAdded = False

	for atime in atimes:

		analysis_time = "%s%02d" % (date,atime)
		delete_time = datetime.datetime.strptime(analysis_time, '%Y%m%d%H') + retention_period

		partition = "%s_%s" % (table_name, analysis_time)
	
		# Check if partition exists in as_grid
		# This is the case when multiple geometries share one table

		if PartitionExists(options, producer_id, geometry_id, partition):
			print "Table partition %s for geometry %s exists already" % (partition, geometry_id)
			continue

		# Check that if as_grid did not have information on this partition, does the
		# table still exist. If so, then we just need to add a row to as_grid.

		query = "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s"

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query, (schema, partition))

		row = cur.fetchone()

		if int(row[0]) == 0:
			partitionAdded = True
			print "Creating partition %s" % (partition)

			query = "CREATE TABLE %s.%s (CHECK (date_trunc('hour', analysis_time) = to_timestamp('%s', 'yyyymmddhh24'))) INHERITS (%s.%s)" % (schema, partition, analysis_time, schema, table_name)

			if options.show_sql:
				print query

			if not options.dry_run:
				cur.execute(query)

		query = "INSERT INTO as_grid (producer_id, analysis_time, geometry_id, delete_time, schema, table_name, partition_name) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s, %s)"

		if options.show_sql:
			print "%s (%s)" % (query, (producer_id, analysis_time, geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), schema, table_name, partition))

		if not options.dry_run:
			cur.execute(query, (producer_id, analysis_time, geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), schema, table_name, partition))


	# Update trigger 

	if partitionAdded:
		CreateTriggers(options, schema, table_name)

	if not options.dry_run:
		conn.commit()

if __name__ == '__main__':

	(options,files) = ReadCommandLine(sys.argv[1:])

	username = 'wetodb'
	password = '3loHRgdio'
	hostname = 'dbdev.fmi.fi'

	print "Connecting to database neon2 at host " + hostname

	conn = psycopg2.connect("user=" + username + " password=" + password + " dbname=neon2 host=" + hostname + " port=5433")

	conn.autocommit = 0

	cur = conn.cursor()

	date = datetime.datetime.now().strftime('%Y%m%d')

	if options.date != None:
		date = options.date

	if options.validate:
		sys.exit(Validate(options, date))

	tables = GetTables(options)

	for element in tables:
		if options.recreate_triggers:
			print "Recreating triggers for table %s" % (element[1])
			CreateTriggers(options, element[5], element[1])
			conn.commit()
		elif options.drop:
			DropTables(options, element)
		else:
			CreateTables(options, element, date)

