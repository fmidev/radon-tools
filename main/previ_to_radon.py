#!/usr/bin/env python

import re
import sys
import datetime
import optparse
import csv
import psycopg2
import bunch
import StringIO
import os

from bunch import Bunch
from optparse import OptionParser

conn = None
cur = None
tableCache = {}
stations = {}

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	desc = "Load previ csv to radon"
	epil ="CSV format is hardcoded and is: "
	epil += "'producer_id,analysis_time,station_id,param_id,level_id,level_value,level_value2,forecast_period,forecast_type_id,forecast_type_value,value'"
	epil += " For example: '2167,2017-04-21 00:00:00,134254,648,6,0,-1,03:00:00,3,1,70'"


	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0", description=desc, epilog=epil)
			  
	parsinggroup = optparse.OptionGroup(parser, "Options for controlling data parsing")
	databasegroup = optparse.OptionGroup(parser, "Options for database connection")

	parser.add_option("-v", "--verbose",
				  action="store_true",
				  default=False,
				  help="Set verbose mode on")

	parsinggroup.add_option("-r", "--producer_id",
					  action="store",
					  type="int",
					  help="Producer id",)

	parsinggroup.add_option("-n", "--network_id",
					  action="store",
					  type="int",
					  default="1",
					  help="Station network id",)

	parsinggroup.add_option("-a", "--analysis_time",
					  action="store",
					  type="string",
					  help="Analysis time, YYYYMMDDHH24MISS")

	parser.add_option("--show_sql",
					  action="store_true",
					  default=False,
					  help="Show SQL that's executed")

	parser.add_option("--dry-run",
					  action="store_true",
					  default=False,
					  help="Do not make any changes to database, sets --show-sql")

	databasegroup.add_option("--host",
					action="store",
					type="string",
					default="vorlon.fmi.fi",
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

	parser.add_option_group(parsinggroup)
	parser.add_option_group(databasegroup)

	(options, arguments) = parser.parse_args()
  
	if options.analysis_time is None:
		print "analysis_time is not set"
		sys.exit(1)
	elif options.producer_id is None:
		print "producer_id is not set"
		sys.exit(1)


	options_dict = vars(options)

	# check that all columns listed with -f are valid

	if options.analysis_time != None:
		try:
			options.analysis_time = datetime.datetime.strptime(options.analysis_time, '%Y%m%d%H%M%S')
		except ValueError,e:
			print e
			sys.exit(1)

	if len(arguments) == 0:
		print "No input files given"
		sys.exit(2)
			
	if options.dry_run:
		options.show_sql = True

	return (options,arguments)

def GetTotalSeconds(td): 
	# no timedelta.total_seconds() in python 2.6
	return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6

def GetTableInfo(options, producer_id, analysis_time):

	key = "%s_%s" % (producer_id, analysis_time)
	try:
		return tableCache[key]
	except:
		pass

	query = """
SELECT
	schema_name,
	table_name,
	partition_name
FROM
	as_previ
WHERE
	producer_id = %s"""

	args = (producer_id,)

	if analysis_time != None:
		query += " AND analysis_time = %s"
		args = args + (analysis_time,)

	if options.show_sql:
		print "%s %s" % (query, args)

	cur.execute(query, args)

	row = cur.fetchone()

	if row == None or len(row) == 0:
		error = "Table not found for producer_id = %s, analysis_time = %s" % (producer_id,analysis_time)
		raise ValueError(error)

	tableInfo = Bunch()

	tableInfo.schema_name = row[0]
	tableInfo.table_name = row[1]

	# If analysis time is not given, use table name as partition name
	# This will be slower since database has to redirect insert to correct partition

	tableInfo.partition_name = row[1]
	
	if analysis_time != None:
		tableInfo.partition_name = row[2]

	tableCache[key] = tableInfo
	return tableInfo

def GetStations(network_id):

	query = """
SELECT 
	station_id,
	local_station_id::int
FROM 
	station_network_mapping
WHERE
	network_id = %s"""

	if options.show_sql:
		print "%s %s" % (query, network_id)

	cur.execute(query, (network_id,))

	rows = cur.fetchall()

	ret = {}

	for row in rows:
		ret[row[1]] = row[0]

	return ret

def Copy(infile, tableInfo):

	with open (infile) as f:

		# check if we have a header in csv
		pos = f.tell()
		line = f.readline()
		f.seek(pos)

		header = True if line[0] == '#' else False

		try:
			cur.execute("SAVEPOINT copy")

			sql = """
COPY %s.%s
  (producer_id,analysis_time,station_id,param_id,level_id,level_value,level_value2,forecast_period,forecast_type_id,forecast_type_value,value) 
FROM STDIN WITH CSV %s DELIMITER AS ','"""

			cur.copy_expert(sql=sql % (tableInfo.schema_name, tableInfo.partition_name, "HEADER" if header else ""), file=f)
			cur.execute("RELEASE SAVEPOINT copy")
			print "Analyzing table %s.%s" % (tableInfo.schema_name, tableInfo.partition_name)

			if options.show_sql:
				print "ANALYZE " + tableInfo.schema_name + "." + tableInfo.partition_name

			if not options.dry_run:
				cur.execute("ANALYZE " + tableInfo.schema_name + "." + tableInfo.partition_name)

			conn.commit()
		except psycopg2.IntegrityError:
			cur.execute("ROLLBACK TO SAVEPOINT copy")
			return False

	return True


def LoadFile(options, infile, stations):

	tableInfo = None

	try:
		tableInfo = GetTableInfo(options, options.producer_id, options.analysis_time)
	except ValueError,e:
		print e
		return

	# First try direct copy which is the fastest way
	# It might fail, in that case do individual inserts
	# and updates.

	if options.network_id == 5:
		# only FMISID network supported in COPY
		if Copy(infile, tableInfo):
			print "Insert with COPY succeeded, whole file uploaded"
			return True

		print "Insert with COPY failed, switching to INSERT"
	Insert(infile, tableInfo, stations)

def Insert(infile, tableInfo, stations):
	
	inserts = 0
	updates = 0

	# prepared statements for insert and update

	query = """
PREPARE insertplan AS
INSERT INTO %s.%s (producer_id, analysis_time, station_id, param_id, level_id, level_value, level_value2, forecast_period, forecast_type_id, forecast_type_value, value)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""" % (tableInfo.schema_name, tableInfo.partition_name)

	if options.show_sql:
		print query

	insertcur = conn.cursor()

	insertcur.execute(query)

	query = """
PREPARE updateplan AS
UPDATE %s.%s SET
	value = $1
WHERE 
	producer_id = $2
	AND
	analysis_time = $3
	AND
	station_id = $4
	AND
	param_id = $5
	AND
	level_id = $6
	AND
	level_value = $7
	AND
	level_value2 = $8
	AND
	forecast_period = $9
	AND
	forecast_type_id = $10
	AND
	forecast_type_value = $11
""" % (tableInfo.schema_name, tableInfo.partition_name) 
	
	if options.show_sql:
		print query
	
	updatecur = conn.cursor()

	updatecur.execute(query)

	# prepared statements are faster when inserting data, but they
	# can only be used when analysis time is static (specified from
	# command line) since the table name is dependent on that time

	start_time = datetime.datetime.now()
	stop_time = None
	max_commit_chunk = 100000
	
	commit_chunk = 200 

	reader = csv.reader(open(infile, 'rb'), delimiter=',')

	buff = []

	totalrows = 0
	for line in reader:

		if len(line) == 0 or line[0][0].strip() == '#':
			continue

		totalrows += 1

		# producer_id,analysis_time,station_id,param_id,level_id,level_value,level_value2,forecast_period,forecast_type_id,forecast_type_value,value

		# convert to fmisids
		try:
			line[2] = stations[int(line[2])]
		except KeyError,e:
			print "No database station id found for local station id %s at network %d" % (line[2], options.network_id)
			continue
		if not options.dry_run:
			cur.execute("SAVEPOINT insert")

			try:
				insertcur.execute("EXECUTE insertplan (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", line)
#				if options.show_sql:
#					print "EXECUTE insertplan (%s, %s, %s, %s)", (cols["meta_id"], cols["analysis_time"], cols["forecast_period"], cols["value"])
				inserts += 1

			except psycopg2.IntegrityError,e:
				if e.pgcode == '23505':
					cur.execute("ROLLBACK TO SAVEPOINT insert")
					line.insert(0, line.pop(len(line)-1))
					updatecur.execute("EXECUTE updateplan (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", line)
					updates += 1
		
			cur.execute("RELEASE SAVEPOINT insert")

		if totalrows % commit_chunk == 0:

			stop_time = datetime.datetime.now()

			print "%s cumulative inserts: %d, updates: %d, lines per second: %d" % (datetime.datetime.now(), inserts, updates, commit_chunk/GetTotalSeconds(stop_time-start_time))

			conn.commit()
	
			start_time = stop_time

	stop_time = datetime.datetime.now()
	print "%s cumulative inserts: %d, updates: %d, lines per second: %d" % (datetime.datetime.now(), inserts, updates, commit_chunk/GetTotalSeconds(stop_time-start_time))

	conn.commit()

	print "%s did %s inserts, %s updates" % (datetime.datetime.now(), inserts, updates)
	print "%s Analyzing table %s.%s" % (datetime.datetime.now(), tableInfo.schema_name, tableInfo.partition_name)

	if options.show_sql:
		print "ANALYZE " + tableInfo.schema_name + "." + tableInfo.partition_name

	if not options.dry_run:
		cur.execute("ANALYZE " + tableInfo.schema_name + "." + tableInfo.partition_name)


# Main body

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

	stations = GetStations(options.network_id)

	for infile in files:
		print "Reading file '%s'" % (infile)
		LoadFile(options, infile, stations)

