import re
import sys
import datetime
import optparse
import csv
import psycopg2
#import ppygis
import collections
import bunch

from bunch import Bunch
from optparse import OptionParser

conn = None
cur = None
metaIds = {}

all_columns = ["producer_id", 
					"analysis_time", 
					"station_id", 
					"param_id", 
					"forecast_period", 
					"latitude", 
					"longitude", 
					"level_id", 
					"level_value",
					"forecast_type_id",
					"forecast_type_value",
					"value"]

# create a list of required columns

required_columns = all_columns
required_columns.remove("forecast_type_value")

# ReadCommandLine()
# 
# Read and parse command line options

def ReadCommandLine(argv):

	desc = """
Load previ csv to neon2.

Program must know the values of the following metadata parameters. Values can be specified either in command line or at csv. Values at command line overwrite csv.

producer_id: Id of producer as found from table fmi_producer. Command line or CSV.
"""

	parser = OptionParser(usage="usage: %prog [options] filename",
						  version="%prog 1.0", description=desc)
			  
	parsinggroup = optparse.OptionGroup(parser, "Options for controlling data parsing")

	parser.add_option("-v", "--verbose",
					  action="store_true",
					  default=False,
					  help="Set verbose mode on")

	parsinggroup.add_option("-r", "--producer_id",
					  action="store",
					  type="int",
					  help="Producer id",)
	
	parsinggroup.add_option("-P", "--forecast_period",
					  action="store",
					  type="string",
					  help="Forecast period, full timestamp (YYYYMMDDHH24MISS) or offset from analysis time in hours",)
	
	parsinggroup.add_option("-p", "--param_id",
					  action="store",
					  type="int",
					  help="Parameter id",)
	
	parsinggroup.add_option("--station_id",
					action="store",
					type="int",
					help="Station id")

	parsinggroup.add_option("--latitude",
					action="store",
					type="float",
					help="Latitude (WGS-84, DD.DDDDD)")

	parsinggroup.add_option("--longitude",
					action="store",
					type="float",
					help="Longitude (WGS-84, DD.DDDDD")

	parsinggroup.add_option("--level_value",
					action="store",
					type="string",
					help="Level value")

	parsinggroup.add_option("--level_id",
					action="store",
					type="int",
					help="Level id")

	parsinggroup.add_option("-f", "--format",
					  action="store",
					  type="string",
					  help="Format (ordering) of CSV columns")

	parsinggroup.add_option("--forecast_type_id",
					  action="store",
					  type="int",
					  help="Forecast type id (from table forecast_type)")

	parsinggroup.add_option("--forecast_type_value",
					  action="store",
					  type="float",
					  help="Forecast type value (optional)")

	parsinggroup.add_option("-a", "--analysis_time",
					  action="store",
					  type="string",
					  help="Analysis time, YYYYMMDDHH24MISS")

	parsinggroup.add_option("--resolve-coordinates-to-station",
					  action="store_true",
					  default=False,
					  help="Try to resolve given latlon values to station id. If station id is not found from database, row will be discarded.")

	parser.add_option("--show_sql",
					  action="store_true",
					  default=False,
					  help="Show SQL that's executed")

	parser.add_option("--dry-run",
					  action="store_true",
					  default=False,
					  help="Do not make any changes to database, sets --show-sql",)

	parser.add_option_group(parsinggroup)
	(options, arguments) = parser.parse_args()
  
	# Check requirements

	if not options.format:
		print "Argument -f must be specified"
		parser.print_help()
		sys.exit(2)

	format_columns = options.format.split(',')

	options_dict = vars(options)

	# check that all columns listed with -f are valid

	for column in format_columns:
		if column not in all_columns:
			print "Unknown column specified: %s" % (column)
			sys.exit(2)

	print format_columns
	for column in set(required_columns) - set(format_columns):
		try:
			if options_dict[column] == None:
				print "CSV column %s not specified with command line argument or with switch -f" % (column)
				sys.exit(2)

		except KeyError:
				print column 
				if column not in ["latitude","longitude","station_id"]:
					print "Accessing CSV column %s that has no corresponding command line option" % (column)
					sys.exit(2)
		
		# check that no unknown columns were given with -f


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

def GetTableInfo(options, producer_id, analysis_time):

	query = """
SELECT
	schema_name,
	table_name,
	partition_name
FROM
	as_previ
WHERE
	producer_id = %s
	AND
	analysis_time = %s"""

	if options.show_sql:
		print "%s %s" % (query, (producer_id,analysis_time))

	cur.execute(query, (producer_id,analysis_time))

	row = cur.fetchone()

	if row == None or len(row) == 0:
		error = "Table not found for producer_id = %s, analysis_time = %s!" % (producer_id,analysis_time)
		raise ValueError(error)

	tableInfo = Bunch()

	tableInfo.schema_name = row[0]
	tableInfo.table_name = row[1]
	tableInfo.partition_name = row[2]

	return tableInfo

def GetStationInfo(latitude, longitude):

	query = """
SELECT 
	s.id, 
	s.name, 
	m.network_id, 
	m.local_station_id 
FROM 
	station s 
LEFT OUTER JOIN 
	station_network_mapping m 
ON (s.id = m.station_id) 
WHERE 
	abs(st_y(location) - %s) < 0.001 
	AND 
	abs(st_x(location) - %s) < 0.001"""

	cur.execute(query, (latitude,longitude))

	rows = cur.fetchall()

	if rows == None or len(rows) == 0:
		error = "Station with latitude = %s, longitude = %s not found!" % (latitude,longitude)
		raise ValueError(error)
  
  	station = Bunch()
  	
  	station.id = rows[0][0]
  	station.name = rows[0][1]
  	station.wmon = None
  	station.icao = None

  	for row in rows:
  		network_id = row[2]

  		if network_id == 1: 
  	 		station.wmon = row[3]
  	 	elif network_id == 2:
  	 		station.icao = row[3]

	return station

def GetMetaIds():

	query = """
SELECT 
	m.id, 
	m.producer_id,
	m.param_id, 
	m.level_id, 
	m.level_value, 
	m.station_id, 
	st_x(m.position) AS longitude, 
	st_y(m.position) AS latitude 
FROM 
	previ_meta m""" 

	if options.show_sql:
		print query

	cur.execute(query)

	rows = cur.fetchall()

	ret = {}

	for row in rows:
		# producer_id_param_id_level_id_level_value_station_id_longitude_latitude
		key = "%s_%s_%s_%s_%s_%s_%s" % (row[1], row[2], row[3], row[4], row[5], row[6], row[7])

		ret[key] = row[0]

	print 'Read ' + str(len(ret)) + ' meta ids' #  for producer %s' % (producer_id)

	return ret

def InsertMetaId(cols):
	query = """
INSERT INTO previ_meta (
	producer_id, 
	param_id,
	level_id,
	level_value,
	station_id,
	position) 
VALUES (%s, %s, %s, %s, %s, ST_PointFromText(%s, 4326))
RETURNING id"""

  	pos = None

  	if cols["latitude"] != None and cols["longitude"] != None:
  		pos = "POINT(%s %s)" % (cols["longitude"], cols["latitude"])
	
	cur.execute(query, (cols["producer_id"], cols["param_id"], cols["level_id"], cols["level_value"], cols["station_id"], pos))
  
	global metaIds
  
	row = cur.fetchone()

	metaIds[str('_'.join(cols))] = row[0]
  
	return row[0]


def ReadFile(options, file):
	
	inserts = 0
	updates = 0

	reader = csv.reader(open(file, 'rb'), delimiter=',')

	totalrows = 0

	producer_id = None

	staticols = {}

	if options.analysis_time != None:
		staticols["analysis_time"] = options.analysis_time.strftime("%Y-%m-%d %H:%M:%S")

	if options.producer_id != None:
		staticols["producer_id"] = options.producer_id

	if options.param_id != None:
		staticols["param_id"] = options.param_id

	if options.forecast_period != None:
		staticols["forecast_period"] = options.forecast_period

	if options.station_id != None:
		staticols["station_id"] = options.station_id

	if options.level_id != None:
		staticols["level_id"] = options.level_id

	if options.level_value != None:
		staticols["level_value"] = options.level_value

	if options.forecast_type_id != None:
		staticols["forecast_type_id"] = options.forecast_type_id

	if options.forecast_type_value != None:
		staticols["forecast_type_value"] = options.forecast_type_value
	
	staticols["forecast_type_value"] = None

	if options.forecast_type_value != None:
		staticols["forecast_type_value"] = options.forecast_type_value

	for line in reader:

		if len(line) == 0 or line[0][0].strip() == '#':
			continue

		totalrows += 1
		
		# ordered dict has keys always in same order when serialized
		# --> our key is guaranteed to be always the same (in same order)

		cols = collections.OrderedDict(staticols)

		i = 0

		for col in options.format.split(','):
			if not col in cols:
				cols[col] = None if line[i].strip() == "" else line[i].strip()
			print col + " --> " + str(line[i])

			i += 1

		# check analysis_time format

		if options.analysis_time is None and not re.match('^\d{14}$', cols["analysis_time"]):
			print "Invalid analysis time format: %s" % (cols["analysis_time"])
			continue

		if (cols["latitude"] != None and cols["longitude"] == None) or (cols["latitude"] == None and cols["longitude"] != None):
			print "Error, both latitude and longitude must be specified, or neither"
			continue

		if cols["station_id"] == None and (cols["latitude"] == None and cols["longitude"] == None):
			print "Error, either station_id or coordinates or both must be specified"
			continue

		if options.resolve_coordinates_to_station:
			if cols["latitude"] == None or cols["latitude"] == None:
				print "Unable to get station info with missing coordinates: %s %s" % (cols["latitude"], cols["longitude"])
			try:	
				stationInfo = GetStationInfo(cols["latitude"],cols["longitude"]);
				cols["station_id"] = stationInfo.id
				cols["latitude"] = None
				cols["longitude"] = None

			except ValueError,e:
				print e
				continue

		# primary key columns must be non-null

		if cols["producer_id"] == None or \
			cols["param_id"] == None or \
			cols["analysis_time"] == None or \
			cols["level_id"] == None or \
			cols["level_value"] == None:
			print "Mandatory columns (producer_id,param_id,analysis_time,level_id,level_value) cannot be NULL"
			print cols
			continue

		tableInfo = None

		try:
			tableInfo = GetTableInfo(options, options.producer_id, cols["analysis_time"])
		except ValueError,e:
			print e
			continue

		# generate key that uniquely identifies a previ_meta id
		# producer_id_param_id_level_id_level_value_station_id_longitude_latitude

		key = "%s_%s_%s_%s_%s_%s_%s" % (options.producer_id, cols["param_id"], cols["level_id"], cols["level_value"], cols["station_id"], cols["longitude"], cols["latitude"])

		print key

		# Forecast period can be full timestamp or offset. In database we only store offset.

		if len(cols["forecast_period"]) == 14:

			atime = None

			if options.analysis_time is not None:
				atime = options.analysis_time
			else:
				atime = datetime.datetime.strptime(cols["analysis_time"], '%Y%m%d%H%M%S')
			
			try:
				per = datetime.datetime.strptime(cols["forecast_period"], '%Y%m%d%H%M%S')
				cols["forecast_period"] = per-atime

			except ValueError,e:
				print e
				continue

		meta_id = None

		try:
			meta_id = metaIds[key]		
		except:
			meta_id = InsertMetaId(cols)

		cur.execute("SAVEPOINT x")

		try:
			query = "INSERT INTO " + tableInfo.schema_name + "." + tableInfo.partition_name + " (previ_meta_id, analysis_time, forecast_period, forecast_type_id, value, forecast_type_value) VALUES (%s, %s, %s, %s, %s, %s)"

			if options.show_sql:
				print "%s %s" % (query, (meta_id, cols["analysis_time"], cols["forecast_period"], cols["forecast_type_id"], cols["value"], cols["forecast_type_value"]))
			
			if not options.dry_run:
				cur.execute(query, (meta_id, cols["analysis_time"], cols["forecast_period"], cols["forecast_type_id"], cols["value"], cols["forecast_type_value"]))

			cur.execute("RELEASE SAVEPOINT x")
			inserts += 1;

		except Exception,e:
			query = "UPDATE " + tableInfo.schema_name + "." + tableInfo.partition_name + " SET value = %s WHERE previ_meta_id = %s AND analysis_time = %s AND forecast_period = %s AND forecast_type_id = %s"
	  
	  		cur.execute("ROLLBACK TO SAVEPOINT x")

	  		if options.show_sql:
				print "%s %s" % (query, (cols["value"], meta_id, cols["analysis_time"], cols["forecast_period"], cols["forecast_type_id"]))

			if not options.dry_run:
				cur.execute(query, (cols["value"], meta_id, cols["analysis_time"], cols["forecast_period"], cols["forecast_type_id"]))
			
			updates += 1;

			if totalrows % 100 == 0:
				print str(datetime.datetime.now()) + " cumulative inserts: " + str(inserts) + ", updates: " + str(updates)
				conn.commit()
	
	conn.commit()
	conn.close()

	print "Did",inserts,"inserts,",updates,"updates"

# Main body

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

	metaIds = GetMetaIds()

	for file in files:
		print "Reading file '%s'" % (file)
		ReadFile(options, file)	
