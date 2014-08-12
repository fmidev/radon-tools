import sys
import datetime
import optparse
import csv
import psycopg2
import re

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

	(options, arguments) = parser.parse_args()
  
	# Check requirements

	if options.date != None and (not re.match("\d{8}", options.date) or len(options.date) != 8):
		print "Invalid format for date: %s" % (options.date)
		print "Should be YYYYMMDD"
		sys.exit(1)
	
	if options.dry_run:
		options.show_sql = True

	return (options,arguments)


# Validate()
# Check that contents of as_grid and the database system tables are in sync

def Validate(options, date):

	tables = GetTables(options)

	if len(tables) == 0:
		print "No records found for producer %d, geometry %d" % (options.producer, options.geometry)
		return

	for element in tables:
		producer = element[0]
		table_name = element[1]
		geometry = element[4]

		print "Producer: %s geometry: %d" % (producer, geometry)

		# Check main table

		query = "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = %s"

		if options.show_sql:
			print "%s %s" % (query, (table_name,))

		cur.execute(query, (table_name,))
			
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Parent table %s does not exist" % (table_name,)
			continue

		# Check that number of childs matches as_grid information

		query = "SELECT count(*) FROM as_grid WHERE table_name = %s"

		if options.show_sql:
			print "%s %s" % (query, (table_name,))

		cur.execute(query, (table_name,))
		
		row = cur.fetchone()

		childs = int(row[0])

		query = "SELECT count(*) FROM pg_inherits i WHERE i.inhparent = 'public.%s'::regclass" % (table_name)

		if options.show_sql:
			print "%s" % (query,)

		cur.execute(query)
		
		row = cur.fetchone()
	
		if int(row[0]) != childs:
			print "Number of child tables for parent %s in as_grid (%d) does not match with database (%d)" % (table_name, childs, int(row[0]))
			continue

		# Check that parent has partitioning trigger

		query = "SELECT count(*) FROM pg_trigger WHERE tgname = '%s_partitioning_trg'" % (table_name)

		if options.show_sql:
			print "%s" % (query)

		cur.execute(query)
		
		row = cur.fetchone()

		if int(row[0]) != 1:
			print "Table %s does not have partitioning trigger defined" % (table_name)
			continue

		print "Parent table %s is valid" % (table_name)

		# Check tables for each analysis times

		for atime in element[3]:
			analysis_time = "%s%02d" % (date,atime)
			partition_name = "%s_%s" % (table_name, analysis_time)

			# Check if partition is in as_grid

			query = "SELECT partition_name, record_count FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND table_name = %s AND partition_name = %s"

			if options.show_sql: 
				print "%s %s" % (query, (producer, geometry,table_name, partition_name))

			cur.execute(query, (producer, geometry,table_name, partition_name))
			
			row = cur.fetchone()

			if row == None:
				print "Partition %s for table %s is not in as_grid" % (partition_name, table_name)
				continue

			record_count = row[1]

			query = "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = %s"

			if options.show_sql:
				print "%s %s" % (query, (partition_name,))

			cur.execute(query, (partition_name,))
			
			row = cur.fetchone()

			if int(row[0]) != 1:
				print "Partition %s is in as_grid but does not exist" % (table_name,)
				continue

			query = "SELECT count(*) FROM " + table_name + " WHERE geometry_id = %s"

			if options.show_sql:
				print "%s %s" % (query, (geometry,))

			cur.execute(query, (geometry,))
			
			row = cur.fetchone()

			if int(row[0]) != record_count:
				print "as_grid reports that record_count for table %s partition %s is %d, but database says it's %d" % (table_name, partition_name, record_count, int(row[0]))
				continue

			print "Partition %s for geometry %d (table %s) is valid" % (partition_name, geometry, table_name)
			

def PartitionExists(options, producer, geometry, partition):
	query = "SELECT count(*) FROM as_grid WHERE producer_id = %s AND geometry_id = %s AND partition_name = %s"

	if options.show_sql:
		print "%s %s" % (query, (producer, geometry, partition)) 
		
	cur.execute(query, (producer, geometry, partition))

	row = cur.fetchone()

	return (int(row[0]) == 1)

def TableExists(options, table):
	query = "SELECT count(*) FROM as_grid WHERE table_name = %s"

	if options.show_sql:
		print "%s (%s)" % (query, table) 

	cur.execute(query, (table,))

	row = cur.fetchone()

	return (int(row[0]) >= 1)

def GetTables(options):

	producer = options.producer
	geometry = options.geometry

	query = "SELECT producer_id, table_name, retention_period, analysis_times, geometry_id FROM table_meta"

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

def CreateTriggers(options, table_name):

	query = """
CREATE OR REPLACE FUNCTION %s_partitioning_f()
RETURNS TRIGGER AS $$
BEGIN
""" % (table_name)

	first = True

	for partition_name in ListPartitions(options, table_name):
		analysis_time = partition_name[-10:]

		ifelsif = "ELSIF"

		if first:
			first = False
			ifelsif = "IF"
		query += """
	%s date_trunc('hour', NEW.analysis_time) = '%s' THEN
		INSERT INTO %s VALUES (NEW.*);""" % (ifelsif, analysis_time, partition_name)

	query += """
	ELSE
		RAISE EXCEPTION 'Partition not found for analysis_time';
	END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER STABLE"""
	
	if options.show_sql:
		print query
	
	if not options.dry_run:
		cur.execute(query)

	query = "DROP TRIGGER IF EXISTS %s_partitioning_trg ON %s" % (table_name, table_name)

	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)

	query = "CREATE TRIGGER %s_partitioning_trg BEFORE INSERT ON %s EXECUTE PROCEDURE %s_partitioning_f()" % (table_name, table_name, table_name)
	
	if options.show_sql:
		print query

	if not options.dry_run:
		cur.execute(query)


def CreateTables(options, element, date):

	atimes = element[3]
	producer_id = element[0]
	table_name = element[1]
	geometry_id = element[4]
	retention_period = element[2]

	print "Producer: %d geometry: %d" % (producer_id, geometry_id) 

	# All DDL on one producer is done in one transaction

	# If we don't add any partitions (meaning that they already exist)
	# there's no need to re-create the triggers

	partitionAdded = False

	for atime in atimes:

		analysis_time = "%s%02d" % (date,atime)
		delete_time = datetime.datetime.strptime(analysis_time, '%Y%m%d%H') + retention_period

		partition_name = "%s_%s" % (table_name, analysis_time)
	
		# Check if partition exists in as_grid
		# This is the case when multiple geometries share one table

		if PartitionExists(options, producer_id, geometry_id, partition_name):
			print "Table partition %s for geometry %s exists already" % (partition_name, geometry_id)
			continue

		if not TableExists(options, table_name):
			print "Parent table %s does not exists, creating" % table_name

			query = "CREATE TABLE %s (LIKE grid_data_template INCLUDING ALL)" % (table_name)

			if options.show_sql:
				print query

			if not options.dry_run:
				cur.execute(query)

		# Check that if as_grid did not have information on this partition, does the 
		# table still exist. If so, then we just need to add a row to as_grid.

		query = "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename = %s"

		if options.show_sql:
			print query

		if not options.dry_run:
			cur.execute(query, (partition_name,))

		row = cur.fetchone()

		if int(row[0]) == 0:
			partitionAdded = True

			query = "CREATE TABLE %s (CHECK (date_trunc('hour', analysis_time) = to_timestamp('%s', 'yyyymmddhh24'))) INHERITS (%s)" % (partition_name, analysis_time, table_name)

			if options.show_sql:
				print query

			if not options.dry_run:
				cur.execute(query)

		query = "INSERT INTO as_grid (producer_id, analysis_time, geometry_id, delete_time, table_name, partition_name) VALUES (%s, to_timestamp(%s, 'yyyymmddhh24'), %s, %s, %s, %s)"

		if options.show_sql:
			print "%s (%s %s %s %s %s %s)" % (query, producer_id, analysis_time, geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), table_name, partition_name)

		if not options.dry_run:
			cur.execute(query, (producer_id, analysis_time, geometry_id, delete_time.strftime('%Y-%m-%d %H:%M:%S'), table_name, partition_name))


	# Update trigger 

	if partitionAdded:
		CreateTriggers(options, table_name)	
	
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
		Validate(options, date)
		sys.exit(0)

	tables = GetTables(options)

	for element in tables:
		if options.recreate_triggers:
			CreateTriggers(options, element[1])	
			conn.commit()
		else:
			CreateTables(options, element, date)

